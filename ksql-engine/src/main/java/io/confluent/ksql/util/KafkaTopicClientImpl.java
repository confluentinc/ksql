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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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


  private boolean isDeleteTopicEnabled = false;

  public KafkaTopicClientImpl(final AdminClient adminClient) {
    this.adminClient = adminClient;
    init();
  }

  @Override
  public void createTopic(
      final String topic,
      final int numPartitions,
      final short replicatonFactor,
      boolean isCompacted
  ) {
    createTopic(topic, numPartitions, replicatonFactor, Collections.emptyMap(), isCompacted);
  }

  @Override
  public void createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor,
      final Map<String, String> configs,
      boolean isCompacted
  ) {
    if (isTopicExists(topic)) {
      validateTopicProperties(topic, numPartitions, replicationFactor);
      return;
    }
    NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
    Map<String, String> newTopicConfigs = new HashMap<>(configs);
    if (isCompacted) {
      newTopicConfigs.put("cleanup.policy", "compact");
    }
    newTopic.configs(newTopicConfigs);
    try {
      log.info("Creating topic '{}'", topic);
      executeWithRetries(() -> adminClient.createTopics(Collections.singleton(newTopic)).all());
    } catch (InterruptedException e) {
      throw new KafkaResponseGetFailedException(
          "Failed to guarantee existence of topic " + topic, e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        // if the topic already exists, it is most likely because another node just created it.
        // ensure that it matches the partition count and replication factor before returning
        // success
        validateTopicProperties(topic, numPartitions, replicationFactor);
        return;
      }
      throw new KafkaResponseGetFailedException("Failed to guarantee existence of topic " + topic,
                                                e);
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
    } catch (InterruptedException | ExecutionException e) {
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
    } catch (InterruptedException | ExecutionException e) {
      throw new KafkaResponseGetFailedException("Failed to Describe Kafka Topics " + topicNames, e);
    }
  }

  @Override
  public Map<String, String> getTopicConfig(final String topicName) {
    return getTopicConfig(topicName, true);
  }

  @Override
  public boolean addTopicConfig(final String topicName, final Map<String, Object> overrides) {
    final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    try {
      final Map<String, String> existingConfig = getTopicConfig(topicName, false);

      final boolean changed = overrides.entrySet().stream()
          .anyMatch(e -> !Objects.equals(existingConfig.get(e.getKey()), e.getValue()));
      if (!changed) {
        return false;
      }

      overrides.forEach((k,v) -> existingConfig.put(k, v.toString()));

      final List<ConfigEntry> entries = existingConfig.entrySet().stream()
          .map(e -> new ConfigEntry(e.getKey(), e.getValue()))
          .collect(Collectors.toList());

      final Map<ConfigResource, Config> request =
          Collections.singletonMap(resource, new Config(entries));

      executeWithRetries(() -> adminClient.alterConfigs(request).all());

      return true;
    } catch (InterruptedException | ExecutionException e) {
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
    List<String> failList = new ArrayList<>();

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
      List<String> internalTopics = new ArrayList<>();
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

  private void init() {
    try {
      DescribeClusterResult describeClusterResult = adminClient.describeCluster();
      List<Node> nodes = new ArrayList<>(describeClusterResult.nodes().get());
      if (!nodes.isEmpty()) {
        ConfigResource resource = new ConfigResource(
            ConfigResource.Type.BROKER,
            String.valueOf(nodes.get(0).id())
        );

        DescribeConfigsResult describeConfigsResult =
            adminClient.describeConfigs(Collections.singleton(resource));

        Map<ConfigResource, Config> config = executeWithRetries(describeConfigsResult::all);

        this.isDeleteTopicEnabled = config.get(resource)
            .entries()
            .stream()
            .anyMatch(configEntry -> configEntry.name().equalsIgnoreCase("delete.topic.enable")
                                     && configEntry.value().equalsIgnoreCase("true"));


      } else {
        log.warn("No available broker found to fetch config info.");
        throw new KsqlException("Could not fetch broker information. KSQL cannot initialize "
                                + "AdminCLient.");
      }
    } catch (InterruptedException | ExecutionException ex) {
      log.error("Failed to initialize TopicClient: {}", ex.getMessage());
      throw new KsqlException("Could not fetch broker information. KSQL cannot initialize "
                              + "AdminClient.", ex);
    }
  }

  private boolean isInternalTopic(final String topicName, String applicationId) {
    return topicName.startsWith(applicationId + "-")
           && (topicName.endsWith("-changelog") || topicName.endsWith("-repartition"));
  }

  public void close() {
    this.adminClient.close();
  }

  private void validateTopicProperties(String topic, int numPartitions, short replicationFactor) {
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
    // Topic with the partitons and replicas exists, reuse it!
    log.debug(
        "Did not create topic {} with {} partitions and replication-factor {} since it already "
        + "exists",
        topic,
        numPartitions,
        replicationFactor
    );
  }

  private Map<String, String> getTopicConfig(final String topicName,
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
    } catch (InterruptedException | ExecutionException e) {
      throw new KafkaResponseGetFailedException(
          "Failed to get config for Kafka Topic " + topicName, e);
    }
  }

  private static <T> T executeWithRetries(
      final Supplier<KafkaFuture<T>> supplier) throws InterruptedException,
                                                      ExecutionException {
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
          log.info("Retrying admin request due to retriable exception. Retry no: "
                   + retries, e);
          lastException = e;
        } else {
          throw e;
        }
      }
    }
    throw new ExecutionException(lastException);
  }
}
