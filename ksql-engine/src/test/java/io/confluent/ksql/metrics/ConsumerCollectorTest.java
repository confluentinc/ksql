package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.metrics.JmxReporter;
import io.confluent.common.metrics.MetricConfig;
import io.confluent.common.metrics.Metrics;
import io.confluent.common.metrics.MetricsReporter;
import io.confluent.common.utils.SystemTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConsumerCollectorTest {

  public static final String TEST_TOPIC = "TestTopic";

  @Test
  public void shouldDisplayRateThroughput() throws Exception {

    Metrics metrics = buildMetrics();

    ConsumerCollector collector = new ConsumerCollector(metrics);

    collector.configure(ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "group"));

    for (int i = 0; i < 1000; i++){

      Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = ImmutableMap.of(
              new TopicPartition(TEST_TOPIC, 1), Arrays.asList(new ConsumerRecord<>(TEST_TOPIC, 1, i, "key", "value")) );
      ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);

      collector.onConsume(consumerRecords);
    }

    String statsForTopic = collector.statsForTopic(TEST_TOPIC);
    assertNotNull(statsForTopic);

    // TODO: ugly - until we determine how to hook it into describe extend (use a describe-metric-reporter)
    assertTrue("Missing data from topic:" + statsForTopic, statsForTopic.contains("name=consumer-group-TestTopic-1-rate-per-sec"));
    assertTrue(statsForTopic.contains("{1=Records:1000"));

  }

  private Metrics buildMetrics() {
    MetricConfig metricConfig = new MetricConfig().samples(100).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter("ksql.metrics"));
    return new Metrics(metricConfig, reporters, new SystemTime());
  }

}