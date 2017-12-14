package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MetricCollectorsTest {

  private static final String TEST_TOPIC = "shared-topic";

  @After
  public void tearDown() {
    MetricCollectors.getMetrics().close();
  }

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

    String firstPassStats = MetricCollectors.getStatsFor(TEST_TOPIC, false);

    assertTrue("Missed stats, got:" + firstPassStats, firstPassStats.contains("total-events:         2"));

    collector2.close();

    collector1.onConsume(consumerRecords);

    String statsForTopic2 =  MetricCollectors.getStatsFor(TEST_TOPIC, false);

    assertTrue("Missed stats, got:" + statsForTopic2, statsForTopic2.contains("total-events:         2"));
  }


  @Test
  public void shouldAggregateStatsAcrossAllProducers() throws Exception {
    ProducerCollector collector1 = new ProducerCollector();
    collector1.configure(ImmutableMap.of(ProducerConfig.CLIENT_ID_CONFIG, "client1"));

    ProducerCollector collector2 = new ProducerCollector();
    collector2.configure(ImmutableMap.of(ProducerConfig.CLIENT_ID_CONFIG, "client2"));

    for (int i = 0; i < 500; i++) {
      collector1.onSend(new ProducerRecord<>(TEST_TOPIC, "key", Integer.toString(i)));
      collector2.onSend(new ProducerRecord<>(TEST_TOPIC + "_" + i, "key",
                                             Integer.toString(i * 100)));
    }

    // The Kafka metrics in MetricCollectors is configured so that sampled stats (like the Rate
    // measurable stat) have a 100 samples, each with a duration of 1 second. In this test we
    // record a 1000 events, but only in a single sample since they all belong to the same second.
    // So 99 samples are empty. Hence the rate is computed as a tenth of what it should be. This
    // won't be a problem for a longer running program.
    assertEquals(10, Math.floor(MetricCollectors.currentProductionRate()), 0);
  }


  @Test
  public void shouldAggregateStatsAcrossAllConsumers() throws Exception {
    ConsumerCollector collector1 = new ConsumerCollector();
    collector1.configure(ImmutableMap.of(ConsumerConfig.CLIENT_ID_CONFIG, "client1"));

    ConsumerCollector collector2 = new ConsumerCollector();
    collector2.configure(ImmutableMap.of(ConsumerConfig.CLIENT_ID_CONFIG, "client2"));
    Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = new HashMap<>();
    List<ConsumerRecord<Object, Object>> recordList = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      recordList.add(new ConsumerRecord<>(TEST_TOPIC, 1, 1,  1l, TimestampType
          .CREATE_TIME,  1l, 10, 10, "key", "1234567890"));
    }
    records.put(new TopicPartition(TEST_TOPIC, 1), recordList);
    ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);
    collector1.onConsume(consumerRecords);
    collector2.onConsume(consumerRecords);

    // Same as the above test, the kafka `Rate` measurable stat reports the rate as a tenth
    // of what it should be because all the samples haven't been filled out yet.
    assertEquals(10, Math.floor(MetricCollectors.currentConsumptionRate()), 0);
  }
}