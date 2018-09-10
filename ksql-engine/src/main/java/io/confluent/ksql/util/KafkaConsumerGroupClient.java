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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

public interface KafkaConsumerGroupClient {

  List<String> listGroups();

  ConsumerGroupSummary describeConsumerGroup(String group);

  /**
   * API POJOs
   */
  class ConsumerGroupSummary {
    final Set<ConsumerSummary> consumerSummaries = new HashSet<>();

    public ConsumerGroupSummary(final Set<ConsumerSummary> summaries) {
      consumerSummaries.addAll(summaries);
    }

    public Collection<ConsumerSummary> consumers() {
      return consumerSummaries;
    }
  }

  class ConsumerSummary {
    final List<TopicPartition> partitions = new ArrayList<>();
    private final String consumerId;

    public ConsumerSummary(final String consumerId) {
      this.consumerId = consumerId;
    }

    public void addPartition(final TopicPartition topicPartition) {
      this.partitions.add(topicPartition);
    }

    public List<TopicPartition> partitions() {
      return partitions;
    }

    void addPartitions(final Set<TopicPartition> topicPartitions) {
      this.partitions.addAll(topicPartitions);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ConsumerSummary that = (ConsumerSummary) o;
      return Objects.equals(consumerId, that.consumerId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(consumerId);
    }
  }
}
