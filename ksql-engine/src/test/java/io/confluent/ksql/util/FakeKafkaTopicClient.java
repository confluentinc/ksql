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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fake Kafka Client is for test only, none of its methods should be called.
 */
public class FakeKafkaTopicClient implements KafkaTopicClient {

  class FakeTopic {
    final String topicName;
    final int numPartitions;
    final short replicatonFactor;

    public FakeTopic(String topicName, int numPartitions, short replicatonFactor) {
      this.topicName = topicName;
      this.numPartitions = numPartitions;
      this.replicatonFactor = replicatonFactor;
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
  }

  Map<String, FakeTopic> topicMap = new HashMap<>();

  @Override
  public void createTopic(String topic, int numPartitions, short replicatonFactor) {
    if (!topicMap.containsKey(topic)) {
      topicMap.put(topic, new FakeTopic(topic, numPartitions, replicatonFactor));
    }
  }

  @Override
  public void createTopic(String topic, int numPartitions, short replicatonFactor, Map<String, String> configs) {
    if (!topicMap.containsKey(topic)) {
      topicMap.put(topic, new FakeTopic(topic, numPartitions, replicatonFactor));
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
  public Map<String, TopicDescription> describeTopics(Collection<String> topicNames) {
    return Collections.emptyMap();
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
