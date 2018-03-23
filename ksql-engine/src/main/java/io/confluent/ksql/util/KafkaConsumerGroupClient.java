/**
 * Copyright 2018 Confluent Inc.
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

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface KafkaConsumerGroupClient extends AutoCloseable {

  List<String> listGroups();

  ConsumerGroupSummary describeConsumerGroup(String group);

  void close();

  /**
   * API POJOs
   */
  class ConsumerGroupSummary {
    final Map<String, ConsumerSummary> consumerSummaries = new HashMap<>();

    public Collection<ConsumerSummary> consumers() {
      return consumerSummaries.values();
    }

    public void addConsumerSummary(ConsumerSummary consumerSummary) {
      this.consumerSummaries.put(consumerSummary.getConsumerId(), consumerSummary);
    }
  }

  class ConsumerSummary {
    final List<TopicPartition> partitions = new ArrayList<>();
    private final String consumerId;

    public ConsumerSummary(String consumerId) {
      this.consumerId = consumerId;
    }

    public void addPartition(TopicPartition topicPartition) {
      this.partitions.add(topicPartition);
    }

    public String getConsumerId() {
      return consumerId;
    }

    public List<TopicPartition> partitions() {
      return partitions;
    }
  }
}
