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

package io.confluent.ksql.test.tools.stubs;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;

import com.google.common.collect.Sets;
import io.confluent.ksql.exception.KafkaTopicExistsException;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.topic.TopicProperties;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * Stub Kafka Client is for test only, none of its methods should be called.
 */
public class StubKafkaTopicClient implements KafkaTopicClient {

  public static class StubTopic {

    private final String topicName;
    private final int numPartitions;
    private final int replicationFactor;
    private final TopicCleanupPolicy cleanupPolicy;

    public StubTopic(
        final String topicName,
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
      final StubTopic stubTopic = (StubTopic) o;
      return numPartitions == stubTopic.numPartitions
          && replicationFactor == stubTopic.replicationFactor
          && Objects.equals(topicName, stubTopic.topicName)
          && cleanupPolicy == stubTopic.cleanupPolicy;
    }

    @Override
    public int hashCode() {
      return Objects.hash(topicName, numPartitions, replicationFactor, cleanupPolicy);
    }
  }

  private final Map<String, StubTopic> topicMap = new HashMap<>();

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

    final StubTopic existing = topicMap.get(topic);
    if (existing != null) {
      validateTopicProperties(numPartitions, replicas, existing);
      return;
    }

    final StubTopic info = createStubTopic(topic, numPartitions, replicas, configs);
    topicMap.put(topic, info);
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
  public Map<String, TopicDescription> describeTopics(final Collection<String> topicNames) {
    return topicNames.stream()
        .collect(Collectors.toMap(Function.identity(), this::describeTopic));
  }

  @Override
  public Map<String, TopicDescription> describeTopics(final Collection<String> topicNames,
                                                      final Boolean isRetryable) {
    return describeTopics(topicNames);
  }

  @Override
  public TopicDescription describeTopic(final String topicName) {
    final StubTopic stubTopic = topicMap.get(topicName);
    if (stubTopic == null) {
      throw new UnknownTopicOrPartitionException("unknown topic: " + topicName);
    }

    return stubTopic.getDescription();
  }

  @Override
  public Map<String, String> getTopicConfig(final String topicName) {
    return Collections.emptyMap();
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
  public Map<TopicPartition, Long> listTopicsOffsets(
      final Collection<String> topicNames,
      final OffsetSpec offsetSpec) {
    return Collections.emptyMap();
  }

  private static StubTopic createStubTopic(
      final String topic,
      final int numPartitions,
      final int replicationFactor,
      final Map<String, ?> configs
  ) {
    final TopicCleanupPolicy cleanUpPolicy =
        CLEANUP_POLICY_COMPACT.equals(configs.get(CLEANUP_POLICY_CONFIG))
            ? TopicCleanupPolicy.COMPACT
            : TopicCleanupPolicy.DELETE;

    return new StubTopic(topic, numPartitions, replicationFactor, cleanUpPolicy);
  }

  private static void validateTopicProperties(
      final int requiredNumPartition,
      final int requiredNumReplicas,
      final StubTopic existing
  ) {
    if (existing.numPartitions != requiredNumPartition
        || (requiredNumReplicas != TopicProperties.DEFAULT_REPLICAS
        && existing.replicationFactor < requiredNumReplicas)) {
      throw new KafkaTopicExistsException(String.format(
          "A Kafka topic with the name '%s' already exists, with different partition/replica "
              + "configuration than required. KSQL expects %d partitions (topic has %d), and %d "
              + "replication factor (topic has %d).",
          existing.topicName,
          requiredNumPartition,
          existing.numPartitions,
          requiredNumReplicas,
          existing.replicationFactor
      ), true);
    }
  }
}
