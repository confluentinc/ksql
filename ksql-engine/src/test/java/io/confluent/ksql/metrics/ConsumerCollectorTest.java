package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.metrics.JmxReporter;
import io.confluent.common.metrics.MetricConfig;
import io.confluent.common.metrics.Metrics;
import io.confluent.common.metrics.MetricsReporter;
import io.confluent.common.utils.SystemTime;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConsumerCollectorTest {

  private static final String TEST_TOPIC = "TestTopic";

  @Test
  public void shouldDisplayRateThroughput() throws Exception {

    ConsumerCollector collector = new ConsumerCollector(new Metrics(), "group");

    for (int i = 0; i < 1000; i++){

      Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = ImmutableMap.of(
              new TopicPartition(TEST_TOPIC, 1), Arrays.asList(
                      new ConsumerRecord<>(TEST_TOPIC, 1, i,  1l, TimestampType.CREATE_TIME,  1l, 10, 10, "key", "1234567890")) );
      ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);

      collector.onConsume(consumerRecords);
    }

    String statsForTopic = collector.statsForTopic(TEST_TOPIC);
    assertNotNull(statsForTopic);

    System.out.println(statsForTopic);

    // TODO: ugly - until we determine how to hook it into describe extend (use a describe-metric-reporter)
    assertTrue("Missing data from topic:" + statsForTopic, statsForTopic.contains("consumer-group-TestTopic-bytes-per-sec-bytes-per-sec:"));
    assertTrue("Missing data from topic:" + statsForTopic, statsForTopic.contains("consumer-group-TestTopic-rate-per-sec-rate-per-sec:"));

  }

}