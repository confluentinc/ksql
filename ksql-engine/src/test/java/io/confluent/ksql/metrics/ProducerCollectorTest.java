package io.confluent.ksql.metrics;

import io.confluent.common.metrics.Metrics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProducerCollectorTest {
  @Test
  public void shouldDisplayRateThroughput() throws Exception {

    ProducerCollector collector = new ProducerCollector(new Metrics(), "clientid");

    for (int i = 0; i < 1000; i++){
      collector.onSend(new ProducerRecord("test-topic", 1, "key", "value"));
    }

    String statsForTopic = collector.statsForTopic("test-topic", true);
    System.out.println(statsForTopic);
    assertNotNull(statsForTopic);

    // TODO: ugly - until we determine how to hook it into describe extend (use a describe-metric-reporter)
    assertTrue(statsForTopic.contains("prod-test-topic--rate-per-sec.rate-per-sec:"));
//    assertTrue(statsForTopic.contains("{1=Records:1000}"));

  }

}