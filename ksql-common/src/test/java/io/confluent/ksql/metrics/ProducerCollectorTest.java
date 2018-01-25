package io.confluent.ksql.metrics;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class ProducerCollectorTest {

  private static final String TEST_TOPIC = "test-topic".toLowerCase();
  @Test
  public void shouldDisplayRateThroughput() throws Exception {

    ProducerCollector collector = new ProducerCollector().configure(new Metrics(), "clientid", MetricCollectors.getTime());

    for (int i = 0; i < 1000; i++){
      collector.onSend(new ProducerRecord(TEST_TOPIC, 1, "key", "value"));
    }

    Collection<TopicSensors.Stat> stats = collector.stats("test-topic", false);

    assertThat( stats.toString(), containsString("name=messages-per-sec,"));
  }

  @Test
  public void shouldRecordErrors() throws Exception {

    ProducerCollector collector = new ProducerCollector().configure(new Metrics(), "clientid", MetricCollectors.getTime());

    for (int i = 0; i < 1000; i++){
      collector.recordError(TEST_TOPIC);
    }

    Collection<TopicSensors.Stat> stats = collector.stats("test-topic", true);

    assertThat( stats.toString(), containsString("failed-messages"));
    assertThat( stats.toString(), containsString("value=1000"));
  }


}
