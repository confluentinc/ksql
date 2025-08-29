/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.services;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.confluent.ksql.topic.TopicProperties;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * Fake Kafka Client is for test only, none of its methods should be called.
 */
public class FakeKafkaTopicClient implements KafkaTopicClient {

  public static class FakeTopic {

    private final String topicName;
    private final int numPartitions;
    private final int replicationFactor;
    private final TopicCleanupPolicy cleanupPolicy;

    public FakeTopic(final String topicName,
        final int numPartitions,
        final int replicationFactor,
        final TopicCleanupPolicy cleanupPolicy) {
      this.topicName = topicName;
      this.numPartitions = numPartitions;
      this.replicationFactor = replicationFactor;
      this.cleanupPolicy = cleanupPolicy;
    }

    private TopicDescription getDescription() {
      final Node node = new Node(0, "localhost", 9091);

      final List<Node> replicas = IntStream.range(0, replicationFactor)
          .mapToObj(idx -> (Node) null)
          .collect(Collectors.toList());

      final List<TopicPartitionInfo> partitionInfoList =
          IntStream.range(0, numPartitions)
              .mapToObj(
                  p -> new TopicPartitionInfo(p, node, replicas, Collections.emptyList()))
              .collect(Collectors.toList());
      return new TopicDescription(
          topicName,
          false,
          partitionInfoList,
          Sets.newHashSet(AclOperation.READ, AclOperation.WRITE)
      );
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final FakeTopic fakeTopic = (FakeTopic) o;
      return numPartitions == fakeTopic.numPartitions
          && replicationFactor == fakeTopic.replicationFactor
          && Objects.equals(topicName, fakeTopic.topicName)
          && cleanupPolicy == fakeTopic.cleanupPolicy;
    }

    @Override
    public int hashCode() {
      return Objects.hash(topicName, numPartitions, replicationFactor, cleanupPolicy);
    }
  }

  private final Map<String, FakeTopic> topicMap = new HashMap<>();
  private final Map<String, Map<String, ?>> topicMapConfig = new HashMap<>();
  private final Map<String, FakeTopic> createdTopics = new HashMap<>();
  private final Map<String, Map<String, ?>> createdTopicsConfig = new HashMap<>();

  public void preconditionTopicExists(
      final String topic
  ) {
    Map<String, ?> configs = ImmutableMap.of(TopicConfig.RETENTION_MS_CONFIG, 604800000L);
    preconditionTopicExists(topic, 1, 1, configs);
  }

  public void preconditionTopicExists(
      final String topic,
      final int numPartitions,
      final int replicationFactor,
      final Map<String, ?> configs) {
    final FakeTopic info = createFakeTopic(topic, numPartitions, replicationFactor, configs);
    topicMap.put(topic, info);
    topicMapConfig.put(topic, configs);
  }

  @Override
  public void createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor,
      final Map<String, ?> configs,
      final CreateTopicsOptions createOptions
  ) {
    final short replicas = replicationFactor == TopicProperties.DEFAULT_REPLICAS
        ? 1
        : replicationFactor;

    final FakeTopic existing = topicMap.get(topic);
    if (existing != null) {
      validateTopicProperties(numPartitions, replicas, existing, configs, getTopicConfig(topic));
      return;
    }

    final FakeTopic info = createFakeTopic(topic, numPartitions, replicas, configs);
    topicMap.put(topic, info);
    createdTopics.put(topic, info);
    createdTopicsConfig.put(topic, configs);
  }

  public Map<String, FakeTopic> createdTopics() {
    return Collections.unmodifiableMap(createdTopics);
  }

  @Override
  public boolean isTopicExists(final String topic) {
    return topicMap.containsKey(topic);
  }

  @Override
  public Set<String> listTopicNames() {
    return topicMap.keySet();
  }

  @Override
  public Map<String, TopicDescription> describeTopics(Collection<String> topicNames,
                                                      Boolean skipRetriesOnFailure) {
    return topicNames.stream()
        .collect(Collectors.toMap(Function.identity(), this::describeTopic));
  }

  @Override
  public TopicDescription describeTopic(final String topicName) {
    final FakeTopic fakeTopic = topicMap.get(topicName);
    if (fakeTopic == null) {
      throw new UnknownTopicOrPartitionException("unknown topic: " + topicName);
    }

    return fakeTopic.getDescription();
  }

  @Override
  public Map<String, String> getTopicConfig(final String topicName) {
    return (Map<String, String>) createdTopicsConfig.getOrDefault(topicName, topicMapConfig.get(topicName));
  }

  @Override
  public boolean addTopicConfig(final String topicName, final Map<String, ?> overrides) {
    return false;
  }

  @Override
  public TopicCleanupPolicy getTopicCleanupPolicy(final String topicName) {
    return topicMap.get(topicName).cleanupPolicy;
  }

  @Override
  public void deleteTopics(final Collection<String> topicsToDelete) {
    for (final String topicName : topicsToDelete) {
      topicMap.remove(topicName);
    }
  }

  @Override
  public void deleteInternalTopics(final String applicationId) {
  }

  @Override
  public Map<TopicPartition, Long> listTopicsOffsets(Collection<String> topicNames,
      OffsetSpec offsetSpec) {
    return Collections.emptyMap();
  }

  private static FakeTopic createFakeTopic(
      final String topic,
      final int numPartitions,
      final int replicationFactor,
      final Map<String, ?> configs
  ) {
    final TopicCleanupPolicy cleanUpPolicy =
        CLEANUP_POLICY_COMPACT.equals(configs.get(CLEANUP_POLICY_CONFIG))
            ? TopicCleanupPolicy.COMPACT
            : TopicCleanupPolicy.DELETE;

    return new FakeTopic(topic, numPartitions, replicationFactor, cleanUpPolicy);
  }

  private static void validateTopicProperties(
      final int requiredNumPartition,
      final int requiredNumReplicas,
      final FakeTopic existing,
      final Map<String, ?> config,
      final Map<String, ?> existingConfig
  ) {
    final Optional<Long> requiredRetentionMs = KafkaTopicClient.getRetentionMs(config);
    final Optional<Long> actualRetentionMs = KafkaTopicClient.getRetentionMs(existingConfig);
    TopicValidationUtil.validateTopicProperties(
        existing.topicName,
        requiredNumPartition,
        requiredNumReplicas,
        requiredRetentionMs,
        existing.numPartitions,
        existing.replicationFactor,
        actualRetentionMs);
  }

}
