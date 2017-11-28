package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConsumerCollectorTest {

  private static final String TEST_TOPIC = "testtopic";

  @Test
  public void shouldDisplayRateThroughput() throws Exception {

    ConsumerCollector collector = new ConsumerCollector(new Metrics(), "group");

    for (int i = 0; i < 100; i++){

      Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = ImmutableMap.of(
              new TopicPartition(TEST_TOPIC, 1), Arrays.asList(
                      new ConsumerRecord<>(TEST_TOPIC, 1, i,  1l, TimestampType.CREATE_TIME,  1l, 10, 10, "key", "1234567890")) );
      ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);

      collector.onConsume(consumerRecords);
    }

    String statsForTopic = collector.statsForTopic(TEST_TOPIC, true);
    assertNotNull(statsForTopic);

    assertTrue("Missing byres-per-sec stat:" + statsForTopic, statsForTopic.contains("bytes-per-sec.bytes-per-sec:"));
    assertTrue("Missing rate-per-sec stat:" + statsForTopic, statsForTopic.contains("rate-per-sec.consume rate-per-sec"));
    assertTrue("total events is wrong:" + statsForTopic, statsForTopic.contains("total-events:    100.00"));
  }


  @Test
  public void shouldHandleConcurrencyToCopyKStreamsThreadBehaviour() throws Exception {

    ExecutorService executorService = Executors.newFixedThreadPool(4);

    ArrayList<ConsumerCollector> collectors = new ArrayList<>();

    Metrics metrics = new Metrics();
    collectors.add(new ConsumerCollector(metrics, "stream-thread-1"));
    collectors.add(new ConsumerCollector(metrics, "stream-thread-2"));
    collectors.add(new ConsumerCollector(metrics, "stream-thread-3"));
    collectors.add(new ConsumerCollector(metrics, "stream-thread-4"));


    for (int i = 0; i < 1000; i++){

      Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = ImmutableMap.of(
              new TopicPartition(TEST_TOPIC, 1), Arrays.asList(
                      new ConsumerRecord<>(TEST_TOPIC, 1, i,  1l, TimestampType.CREATE_TIME,  1l, 10, 10, "key", "1234567890")) );
      ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);

      final int index = i % collectors.size();
      executorService.submit(() -> collectors.get(index).onConsume(consumerRecords));
    }

    executorService.shutdown();
    executorService.awaitTermination(10, TimeUnit.SECONDS);

    String statsForTopic = collectors.get(0).statsForTopic(TEST_TOPIC, true);
    assertNotNull(statsForTopic);

    assertTrue("totalevents is wrong:" + statsForTopic, statsForTopic.contains("total-events:   1000.00"));
  }
}