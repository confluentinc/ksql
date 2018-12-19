/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.COMPRESSION_TYPE_CONFIG;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

/**
 * Fake Kafka Client is for test only, none of its methods should be called.
 */
public class FakeKafkaTopicClient implements KafkaTopicClient {

  static class FakeTopic {
    private final String topicName;
    private final int numPartitions;
    private final short replicatonFactor;
    private final TopicCleanupPolicy cleanupPolicy;

    public FakeTopic(final String topicName,
        final int numPartitions,
        final short replicatonFactor,
        final TopicCleanupPolicy cleanupPolicy) {
      this.topicName = topicName;
      this.numPartitions = numPartitions;
      this.replicatonFactor = replicatonFactor;
      this.cleanupPolicy = cleanupPolicy;
    }

    public String getTopicName() {
      return topicName;
    }

    public int getNumPartitions() {
      return numPartitions;
    }

    public short getReplicatonFactor() {
      return replicatonFactor;
    }

    public TopicDescription getDescription() {
      final Node node = new Node(0, "localhost", 9091);
      final List<TopicPartitionInfo> partitionInfoList =
          IntStream.range(0, numPartitions)
              .mapToObj(
                  p -> new TopicPartitionInfo(p, node, Collections.emptyList(), Collections.emptyList()))
              .collect(Collectors.toList());
      return new TopicDescription(topicName, false, partitionInfoList);
    }
    public TopicCleanupPolicy getCleanupPolicy() {
      return cleanupPolicy;
    }
  }

  private final Map<String, FakeTopic> topicMap = new HashMap<>();
  private final Map<String, FakeTopic> createdTopics = new HashMap<>();

  public void preconditionTopicExists(
      final String topic,
      final int numPartitions,
      final short replicationFactor,
      final Map<String, ?> configs) {
    topicMap.put(topic, createFakeTopic(topic, numPartitions, replicationFactor, configs));
  }

  @Override
  public void createTopic(final String topic, final int numPartitions, final short replicationFactor, final Map<String, ?> configs) {
    if (topicMap.containsKey(topic)) {
      return;
    }

    final FakeTopic info = createFakeTopic(topic, numPartitions, replicationFactor, configs);
    topicMap.put(topic, info);
    createdTopics.put(topic, info);
  }

  public Map<String, FakeTopic> createdTopics() {
    return createdTopics;
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
  public Set<String> listNonInternalTopicNames() {
    return topicMap.keySet().stream()
        .filter((topic) -> (!topic.startsWith(KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX)
            || !topic.startsWith(KsqlConstants.CONFLUENT_INTERNAL_TOPIC_PREFIX)))
        .collect(Collectors.toSet());
  }

  @Override
  public Map<String, TopicDescription> describeTopics(final Collection<String> topicNames) {
    return listTopicNames()
        .stream()
        .filter(topicNames::contains)
        .collect(
            Collectors.toMap(n -> n, n -> topicMap.get(n).getDescription()));
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
    return null;
  }

  @Override
  public void deleteTopics(final List<String> topicsToDelete) {
    for (final String topicName: topicsToDelete) {
      topicMap.remove(topicName);
    }
  }

  @Override
  public void deleteInternalTopics(final String applicationId) {
  }

  private static FakeTopic createFakeTopic(final String topic, final int numPartitions,
      final short replicationFactor, final Map<String, ?> configs) {
    final TopicCleanupPolicy cleanUpPolicy =
        CLEANUP_POLICY_COMPACT.equals(configs.get(COMPRESSION_TYPE_CONFIG))
            ? TopicCleanupPolicy.COMPACT
            : TopicCleanupPolicy.DELETE;

    return new FakeTopic(topic, numPartitions, replicationFactor, cleanUpPolicy);
  }
}