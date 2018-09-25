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
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KafkaTopicException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicClientImpl implements KafkaTopicClient {

  private static final Logger log = LoggerFactory.getLogger(KafkaTopicClient.class);

  private final AdminClient adminClient;
  private final boolean isDeleteTopicEnabled;

  /**
   * Construct a topic client from an existing admin client.
   *
   * @param adminClient the admin client. Note: Will be closed on {@link #close()}.
   */
  public KafkaTopicClientImpl(final AdminClient adminClient) {
    this.adminClient = Objects.requireNonNull(adminClient, "adminClient");
    this.isDeleteTopicEnabled = isTopicDeleteEnabled(adminClient);
  }

  @Override
  public void createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor,
      final boolean checkTopicProperties,
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
      ExecutorUtil.executeWithRetries(
          () -> adminClient.createTopics(Collections.singleton(newTopic)).all().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE);
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
      return ExecutorUtil.executeWithRetries(
          () -> adminClient.listTopics().names().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE);
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
      return ExecutorUtil.executeWithRetries(
          () -> adminClient.describeTopics(topicNames).all().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE);
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

      ExecutorUtil.executeWithRetries(
          () -> adminClient.alterConfigs(request).all().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE);

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
    final List<String> failList = Lists.newArrayList();

    for (final Map.Entry<String, KafkaFuture<Void>> entry : results.entrySet()) {
      try {
        entry.getValue().get(30, TimeUnit.SECONDS);
      } catch (final Exception e) {
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
      final Set<String> topicNames = listTopicNames();
      final List<String> internalTopics = Lists.newArrayList();
      for (final String topicName : topicNames) {
        if (isInternalTopic(topicName, applicationId)) {
          internalTopics.add(topicName);
        }
      }
      if (!internalTopics.isEmpty()) {
        deleteTopics(internalTopics);
      }
    } catch (final Exception e) {
      log.error("Exception while trying to clean up internal topics for application id: {}.",
                applicationId, e
      );
    }
  }

  private static boolean isTopicDeleteEnabled(final AdminClient adminClient) {
    try {
      final DescribeClusterResult describeClusterResult = adminClient.describeCluster();
      final Collection<Node> nodes = describeClusterResult.nodes().get();
      if (nodes.isEmpty()) {
        log.warn("No available broker found to fetch config info.");
        throw new KsqlException("Could not fetch broker information. KSQL cannot initialize");
      }

      final ConfigResource resource = new ConfigResource(
          ConfigResource.Type.BROKER,
          String.valueOf(nodes.iterator().next().id())
      );

      final Map<ConfigResource, Config> config = ExecutorUtil.executeWithRetries(
          () -> adminClient.describeConfigs(Collections.singleton(resource)).all().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE);

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

  private void validateTopicProperties(final String topic,
                                       final int numPartitions,
                                       final short replicationFactor) {
    final Map<String, TopicDescription> topicDescriptions =
        describeTopics(Collections.singletonList(topic));
    final TopicDescription topicDescription = topicDescriptions.get(topic);
    if (topicDescription.partitions().size() != numPartitions
        || topicDescription.partitions().get(0).replicas().size() < replicationFactor) {
      throw new KafkaTopicException(String.format(
              "A Kafka topic with the name '%s' already exists, with different partition/replica "
              + "configuration than required. KSQL expects %d partitions (topic has %d), and %d "
              + "replication factor (topic has %d).",
              topic,
              numPartitions,
              topicDescription.partitions().size(),
              replicationFactor,
              topicDescription.partitions().get(0).replicas().size()
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
      final Config config = ExecutorUtil.executeWithRetries(
          () -> adminClient.describeConfigs(request).all().get(),
          ExecutorUtil.RetryBehaviour.ON_RETRYABLE).get(resource);
      return config.entries().stream()
          .filter(e -> includeDefaults
                       || e.source().equals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG))
          .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
    } catch (final Exception e) {
      throw new KafkaResponseGetFailedException(
          "Failed to get config for Kafka Topic " + topicName, e);
    }
  }

  private static Map<String, String> toStringConfigs(final Map<String, ?> configs) {
    return configs.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
  }
}
