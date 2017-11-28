package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class MetricCollectorsTest {

  private static final String TEST_TOPIC = "testtopic";
  private static final String TEST_TOPIC_2 = "testtopic2";

  @Test
  public void getConsumerStatsByTopic() throws Exception {


    MetricCollector metricCollector = new MetricCollector();
    metricCollector.configure(ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group"));

    Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = ImmutableMap.of(
            new TopicPartition(TEST_TOPIC, 1), Arrays.asList(
                    new ConsumerRecord<>(TEST_TOPIC, 1, 1,  1l, TimestampType.CREATE_TIME,  1l, 10, 10, "key", "1234567890")) );
    ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);
    metricCollector.onConsume(consumerRecords);
    String statsForTopic = metricCollector.statsForTopic(TEST_TOPIC);
    assertTrue(statsForTopic.contains("total-events:      1.00"));
  }


  @Test
  public void getProducerStatsByTopic() throws Exception {

    MetricCollector metricCollector = new MetricCollector();
    metricCollector.configure(ImmutableMap.of(ProducerConfig.CLIENT_ID_CONFIG, "stream-thread-one"));
    metricCollector.onSend(new ProducerRecord(TEST_TOPIC, 1, "key", "value"));
    String statsForTopic = metricCollector.statsForTopic(TEST_TOPIC);
    assertTrue(statsForTopic.contains("total-events:      1.00"));
  }

  @Test
  public void shouldKeepWorkingWhenDuplicateTopicConsumerRemoved() throws Exception {


    MetricCollector metricCollector1 = new MetricCollector();
    metricCollector1.configure(ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1"));

    MetricCollector metricCollector2 = new MetricCollector();
    metricCollector2.configure(ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-2"));


    Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = ImmutableMap.of(
            new TopicPartition(TEST_TOPIC_2, 1), Arrays.asList(
                    new ConsumerRecord<>(TEST_TOPIC_2, 1, 1,  1l, TimestampType.CREATE_TIME,  1l, 10, 10, "key", "1234567890")) );
    ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);


    metricCollector1.onConsume(consumerRecords);
    metricCollector2.onConsume(consumerRecords);

    String firstPassStats = metricCollector1.statsForTopic(TEST_TOPIC_2);

    assertTrue("Missed stats, got:" + firstPassStats, firstPassStats.contains("total-events:      2.00"));

    Metrics metrics = MetricCollectors.getMetrics();

    metricCollector2.close();

    metricCollector1.onConsume(consumerRecords);

    String statsForTopic2 = metricCollector1.statsForTopic(TEST_TOPIC_2);

    assertTrue("Missed stats, got:" + statsForTopic2, statsForTopic2.contains("total-events:      3.00"));

  }
}