package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MetricCollectorsTest {

  private static final String TEST_TOPIC = "shared-topic";

  @Test
  public void shouldAggregateStats() throws Exception {

    List<TopicSensors.Stat> stats = Arrays.asList(new TopicSensors.Stat("metric", 1, 1l), new TopicSensors.Stat("metric", 1, 1l), new TopicSensors.Stat("metric", 1, 1l));
    Map<String, TopicSensors.Stat> aggregateMetrics = MetricCollectors.getAggregateMetrics(stats);
    assertThat(aggregateMetrics.size(), equalTo(1));
    assertThat(aggregateMetrics.values().iterator().next().getValue(), equalTo(3.0));
  }


    @Test
  public void shouldKeepWorkingWhenDuplicateTopicConsumerIsRemoved() throws Exception {

    ConsumerCollector collector1 = new ConsumerCollector();
    collector1.configure(ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "stream-thread-1") );

    ConsumerCollector collector2 = new ConsumerCollector();
    collector2.configure(ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "stream-thread-2") );



    Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = ImmutableMap.of(
            new TopicPartition(TEST_TOPIC, 1), Arrays.asList(
                    new ConsumerRecord<>(TEST_TOPIC, 1, 1,  1l, TimestampType.CREATE_TIME,  1l, 10, 10, "key", "1234567890")) );
    ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);


    collector1.onConsume(consumerRecords);
    collector2.onConsume(consumerRecords);

    String firstPassStats = MetricCollectors.getStatsFor(TEST_TOPIC);

    assertTrue("Missed stats, got:" + firstPassStats, firstPassStats.contains("total-events:      2.00"));

    collector2.close();

    collector1.onConsume(consumerRecords);

    String statsForTopic2 =  MetricCollectors.getStatsFor(TEST_TOPIC);

    assertTrue("Missed stats, got:" + statsForTopic2, statsForTopic2.contains("total-events:      2.00"));

  }
}