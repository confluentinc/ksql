/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import com.google.common.collect.Lists;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KafkaTopicException;

public class KafkaTopicClientImpl implements KafkaTopicClient {

  private static final Logger log = LoggerFactory.getLogger(KafkaTopicClient.class);
  private static final int NUM_RETRIES = 5;
  private static final int RETRY_BACKOFF_MS = 500;

  private final AdminClient adminClient;
  private final boolean isDeleteTopicEnabled;

  public KafkaTopicClientImpl(final AdminClient adminClient) {
    this.adminClient = adminClient;
    this.isDeleteTopicEnabled = isTopicDeleteEnabled(adminClient);
  }

  @Override
  public void createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor
  ) {
    createTopic(topic, numPartitions, replicationFactor, Collections.emptyMap());
  }

  @Override
  public void createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor,
      final Map<String, ?> configs
  ) {
    if (isTopicExists(topic)) {
      validateTopicProperties(topic, numPartitions, replicationFactor);
      return;
    }

    final NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
    newTopic.configs(toStringConfigs(configs));

    try {
      log.info("Creating topic '{}'", topic);
      executeWithRetries(() -> adminClient.createTopics(Collections.singleton(newTopic)).all());

    } catch (final InterruptedException e) {
      throw new KafkaResponseGetFailedException(
          "Failed to guarantee existence of topic " + topic, e);

    } catch (final TopicExistsException e) {
      // if the topic already exists, it is most likely because another node just created it.
      // ensure that it matches the partition count and replication factor before returning
      // success
      validateTopicProperties(topic, numPartitions, replicationFactor);

    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException(
          "Failed to guarantee existence of topic " + topic, e);
    }
  }

  @Override
  public boolean isTopicExists(final String topic) {
    log.trace("Checking for existence of topic '{}'", topic);
    return listTopicNames().contains(topic);
  }

  @Override
  public Set<String> listTopicNames() {
    try {
      return executeWithRetries(() -> adminClient.listTopics().names());
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException("Failed to retrieve Kafka Topic names", e);
    }
  }

  @Override
  public Set<String> listNonInternalTopicNames() {
    return listTopicNames().stream()
        .filter((topic) -> !(topic.startsWith(KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX)
                             || topic.startsWith(KsqlConstants.CONFLUENT_INTERNAL_TOPIC_PREFIX)))
        .collect(Collectors.toSet());
  }

  @Override
  public Map<String, TopicDescription> describeTopics(final Collection<String> topicNames) {
    try {
      return executeWithRetries(() -> adminClient.describeTopics(topicNames).all());
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException("Failed to Describe Kafka Topics " + topicNames, e);
    }
  }

  @Override
  public Map<String, String> getTopicConfig(final String topicName) {
    return topicConfig(topicName, true);
  }

  @Override
  public boolean addTopicConfig(final String topicName, final Map<String, ?> overrides) {
    final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    try {
      final Map<String, String> existingConfig = topicConfig(topicName, false);

      final boolean changed = overrides.entrySet().stream()
          .anyMatch(e -> !Objects.equals(existingConfig.get(e.getKey()), e.getValue()));
      if (!changed) {
        return false;
      }

      existingConfig.putAll(toStringConfigs(overrides));

      final Set<ConfigEntry> entries = existingConfig.entrySet().stream()
          .map(e -> new ConfigEntry(e.getKey(), e.getValue()))
          .collect(Collectors.toSet());

      final Map<ConfigResource, Config> request =
          Collections.singletonMap(resource, new Config(entries));

      executeWithRetries(() -> adminClient.alterConfigs(request).all());

      return true;
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException(
          "Failed to set config for Kafka Topic " + topicName, e);
    }
  }

  @Override
  public TopicCleanupPolicy getTopicCleanupPolicy(final String topicName) {
    final String policy = getTopicConfig(topicName)
        .getOrDefault(TopicConfig.CLEANUP_POLICY_CONFIG, "");

    switch (policy) {
      case "compact":
        return TopicCleanupPolicy.COMPACT;
      case "delete":
        return TopicCleanupPolicy.DELETE;
      case "compact+delete":
        return TopicCleanupPolicy.COMPACT_DELETE;
      default:
        throw new KsqlException("Could not get the topic configs for : " + topicName);
    }
  }

  @Override
  public void deleteTopics(final List<String> topicsToDelete) {
    if (!isDeleteTopicEnabled) {
      log.info("Cannot delete topics since 'delete.topic.enable' is false. ");
      return;
    }
    final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicsToDelete);
    final Map<String, KafkaFuture<Void>> results = deleteTopicsResult.values();
    List<String> failList = Lists.newArrayList();

    for (final Map.Entry<String, KafkaFuture<Void>> entry : results.entrySet()) {
      try {
        entry.getValue().get(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        failList.add(entry.getKey());
      }
    }
    if (!failList.isEmpty()) {
      throw new KsqlException("Failed to clean up topics: " + failList.stream()
          .collect(Collectors.joining(",")));
    }
  }

  @Override
  public void deleteInternalTopics(final String applicationId) {
    if (!isDeleteTopicEnabled) {
      log.warn("Cannot delete topics since 'delete.topic.enable' is false. ");
      return;
    }
    try {
      Set<String> topicNames = listTopicNames();
      List<String> internalTopics = Lists.newArrayList();
      for (String topicName : topicNames) {
        if (isInternalTopic(topicName, applicationId)) {
          internalTopics.add(topicName);
        }
      }
      if (!internalTopics.isEmpty()) {
        deleteTopics(internalTopics);
      }
    } catch (Exception e) {
      log.error("Exception while trying to clean up internal topics for application id: {}.",
                applicationId, e
      );
    }
  }

  private static boolean isTopicDeleteEnabled(final AdminClient adminClient) {
    try {
      DescribeClusterResult describeClusterResult = adminClient.describeCluster();
      Collection<Node> nodes = describeClusterResult.nodes().get();
      if (nodes.isEmpty()) {
        log.warn("No available broker found to fetch config info.");
        throw new KsqlException("Could not fetch broker information. KSQL cannot initialize");
      }

      ConfigResource resource = new ConfigResource(
          ConfigResource.Type.BROKER,
          String.valueOf(nodes.iterator().next().id())
      );

      Map<ConfigResource, Config> config = executeWithRetries(
          () -> adminClient.describeConfigs(Collections.singleton(resource)).all());

      return config.get(resource)
          .entries()
          .stream()
          .anyMatch(configEntry -> configEntry.name().equalsIgnoreCase("delete.topic.enable")
                                   && configEntry.value().equalsIgnoreCase("true"));

    } catch (final Exception e) {
      log.error("Failed to initialize TopicClient: {}", e.getMessage());
      throw new KsqlException("Could not fetch broker information. KSQL cannot initialize", e);
    }
  }

  private boolean isInternalTopic(final String topicName, final String applicationId) {
    return topicName.startsWith(applicationId + "-")
           && (topicName.endsWith(KsqlConstants.STREAMS_CHANGELOG_TOPIC_SUFFIX)
               || topicName.endsWith(KsqlConstants.STREAMS_REPARTITION_TOPIC_SUFFIX));
  }

  public void close() {
    this.adminClient.close();
  }

  private void validateTopicProperties(final String topic,
                                       final int numPartitions,
                                       final short replicationFactor) {
    Map<String, TopicDescription> topicDescriptions =
        describeTopics(Collections.singletonList(topic));
    TopicDescription topicDescription = topicDescriptions.get(topic);
    if (topicDescription.partitions().size() != numPartitions
        || topicDescription.partitions().get(0).replicas().size() < replicationFactor) {
      throw new KafkaTopicException(String.format(
          "Topic '%s' does not conform to the requirements Partitions:%d v %d. Replication: %d "
          + "v %d",
          topic,
          topicDescription.partitions().size(),
          numPartitions,
          topicDescription.partitions().get(0).replicas().size(),
          replicationFactor
      ));
    }
    // Topic with the partitions and replicas exists, reuse it!
    log.debug(
        "Did not create topic {} with {} partitions and replication-factor {} since it already "
        + "exists",
        topic,
        numPartitions,
        replicationFactor
    );
  }

  private Map<String, String> topicConfig(final String topicName,
                                          final boolean includeDefaults) {
    final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    final List<ConfigResource> request = Collections.singletonList(resource);

    try {
      final Config config = executeWithRetries(() -> adminClient.describeConfigs(request).all())
          .get(resource);

      return config.entries().stream()
          .filter(e -> includeDefaults
                       || e.source().equals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG))
          .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException(
          "Failed to get config for Kafka Topic " + topicName, e);
    }
  }

  private static Map<String, String> toStringConfigs(Map<String, ?> configs) {
    return configs.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
  }

  private static <T> T executeWithRetries(final Supplier<KafkaFuture<T>> supplier)
      throws Exception {

    int retries = 0;
    Exception lastException = null;
    while (retries < NUM_RETRIES) {
      try {
        if (retries != 0) {
          Thread.sleep(RETRY_BACKOFF_MS);
        }
        return supplier.get().get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof RetriableException) {
          retries++;
          log.info("Retrying admin request due to retriable exception. Retry no: " + retries, e);
          lastException = e;
        } else if (e.getCause() instanceof Exception) {
          throw (Exception) e.getCause();
        } else {
          throw e;
        }
      }
    }
    throw lastException;
  }
}
