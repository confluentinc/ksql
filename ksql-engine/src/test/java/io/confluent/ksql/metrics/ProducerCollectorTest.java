package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.metrics.JmxReporter;
import io.confluent.common.metrics.MetricConfig;
import io.confluent.common.metrics.Metrics;
import io.confluent.common.metrics.MetricsReporter;
import io.confluent.common.utils.SystemTime;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ProducerCollectorTest {
  @Test
  public void shouldDisplayRateThroughput() throws Exception {

    Metrics metrics = buildMetrics();

    ProducerCollector collector = new ProducerCollector(metrics);

    collector.configure(ImmutableMap.of(ProducerConfig.CLIENT_ID_CONFIG, "clientid"));

    for (int i = 0; i < 1000; i++){
      collector.onSend(new ProducerRecord("test-topic", 1, "key", "value"));
    }

    String statsForTopic = collector.statsForTopic("test-topic");
    System.out.println(statsForTopic);
    assertNotNull(statsForTopic);

    // TODO: ugly - until we determine how to hook it into describe extend (use a describe-metric-reporter)
    assertTrue(statsForTopic.contains("name=producer-clientid-test-topic-rate"));
    assertTrue(statsForTopic.contains("{1=Records:1000}"));

  }

  private Metrics buildMetrics() {
    MetricConfig metricConfig = new MetricConfig().samples(100).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter("ksql.metrics"));
    return new Metrics(metricConfig, reporters, new SystemTime());
  }

}