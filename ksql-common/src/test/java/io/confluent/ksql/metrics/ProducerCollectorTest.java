package io.confluent.ksql.metrics;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collection;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class ProducerCollectorTest {

  private static final String TEST_TOPIC = "test-topic".toLowerCase();
  @Test
  public void shouldDisplayRateThroughput() {

    final ProducerCollector collector = new ProducerCollector().configure(new Metrics(), "clientid", MetricCollectors.getTime());

    for (int i = 0; i < 1000; i++){
      collector.onSend(new ProducerRecord(TEST_TOPIC, 1, "key", "value"));
    }

    final Collection<TopicSensors.Stat> stats = collector.stats("test-topic", false);

    assertThat( stats.toString(), containsString("name=messages-per-sec,"));
  }
}
