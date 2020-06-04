package io.confluent.ksql.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class FakeKafkaConsumerGroupClient implements KafkaConsumerGroupClient {

  private static final List<String> groups = ImmutableList.of("cg1", "cg2");

  @Override
  public List<String> listGroups() {
    return groups;
  }

  @Override
  public ConsumerGroupSummary describeConsumerGroup(String group) {
    if (groups.contains(group)) {
      Set<ConsumerSummary> instances = ImmutableSet.of(
          new ConsumerSummary(group + "-1"),
          new ConsumerSummary(group + "-2")
      );
      return new ConsumerGroupSummary(instances);
    } else {
      return null;
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String group) {
    if (groups.contains(group)) {
      Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
      offsets.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(10));
      offsets.put(new TopicPartition("topic1", 1), new OffsetAndMetadata(11));
      return offsets;
    } else {
      return Collections.emptyMap();
    }
  }
}
