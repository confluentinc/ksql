/**
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

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.COMPRESSION_TYPE_CONFIG;

/**
 * Fake Kafka Client is for test only, none of its methods should be called.
 */
public class FakeKafkaTopicClient implements KafkaTopicClient {

  class FakeTopic {
    private final String topicName;
    private final int numPartitions;
    private final short replicatonFactor;
    private final TopicCleanupPolicy cleanupPolicy;

    public FakeTopic(String topicName,
                     int numPartitions,
                     short replicatonFactor,
                     TopicCleanupPolicy cleanupPolicy) {
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
      Node node = new Node(0, "localhost", 9091);
      List<TopicPartitionInfo> partitionInfoList =
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

  private Map<String, FakeTopic> topicMap = new HashMap<>();

  @Override
  public void createTopic(String topic, int numPartitions, short replicationFactor) {
    if (!topicMap.containsKey(topic)) {
      topicMap.put(topic, new FakeTopic(topic, numPartitions, replicationFactor, TopicCleanupPolicy.DELETE));
    }
  }

  @Override
  public void createTopic(String topic, int numPartitions, short replicationFactor, Map<String, ?> configs) {
    if (!topicMap.containsKey(topic)) {
      final TopicCleanupPolicy cleanUpPolicy =
          CLEANUP_POLICY_COMPACT.equals(configs.get(COMPRESSION_TYPE_CONFIG))
          ? TopicCleanupPolicy.COMPACT
          : TopicCleanupPolicy.DELETE;

      topicMap.put(topic, new FakeTopic(topic, numPartitions, replicationFactor, cleanUpPolicy));
    }
  }

  @Override
  public boolean isTopicExists(String topic) {
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
  public Map<String, TopicDescription> describeTopics(Collection<String> topicNames) {
    return listTopicNames()
        .stream()
        .filter(n -> topicNames.contains(n))
        .collect(
            Collectors.toMap(n -> n, n -> topicMap.get(n).getDescription()));
  }

  @Override
  public Map<String, String> getTopicConfig(String topicName) {
    return Collections.emptyMap();
  }

  @Override
  public boolean addTopicConfig(String topicName, Map<String, ?> overrides) {
    return false;
  }

  @Override
  public TopicCleanupPolicy getTopicCleanupPolicy(String topicName) {
    return null;
  }

  @Override
  public void deleteTopics(List<String> topicsToDelete) {
    for (String topicName: topicsToDelete) {
      topicMap.remove(topicName);
    }
  }

  @Override
  public void deleteInternalTopics(String applicationId) {
  }

  @Override
  public void close() {  }

}
