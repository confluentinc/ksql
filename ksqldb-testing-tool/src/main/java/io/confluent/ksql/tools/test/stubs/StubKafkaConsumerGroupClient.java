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

package io.confluent.ksql.tools.test.stubs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.services.KafkaConsumerGroupClient;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class StubKafkaConsumerGroupClient implements KafkaConsumerGroupClient {

  private static final List<String> groups = ImmutableList.of("cg1", "cg2");

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "groups is ImmutableList")
  public List<String> listGroups() {
    return groups;
  }

  @Override
  public ConsumerGroupSummary describeConsumerGroup(final String group) {
    if (groups.contains(group)) {
      final Set<ConsumerSummary> instances = ImmutableSet.of(
          new ConsumerSummary(group + "-1"),
          new ConsumerSummary(group + "-2")
      );
      return new ConsumerGroupSummary(instances);
    } else {
      throw new KafkaResponseGetFailedException(
          "Failed to retrieve Kafka consumer groups",
          new RuntimeException()
      );
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(final String group) {
    if (groups.contains(group)) {
      final Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
      offsets.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(10));
      offsets.put(new TopicPartition("topic1", 1), new OffsetAndMetadata(11));
      return offsets;
    } else {
      throw new KafkaResponseGetFailedException(
          "Failed to retrieve Kafka consumer groups",
          new RuntimeException()
      );
    }
  }

  @Override
  public void deleteConsumerGroups(final Set<String> groups) {
    // do nothing
  }
}
