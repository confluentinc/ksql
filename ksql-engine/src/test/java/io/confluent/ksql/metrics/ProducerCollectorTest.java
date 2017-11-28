package io.confluent.ksql.metrics;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProducerCollectorTest {

  private static final String TEST_TOPIC = "test-topic".toLowerCase();
  @Test
  public void shouldDisplayRateThroughput() throws Exception {

    ProducerCollector collector = new ProducerCollector(new Metrics(), "clientid");

    for (int i = 0; i < 1000; i++){
      collector.onSend(new ProducerRecord(TEST_TOPIC, 1, "key", "value"));
    }

    String statsForTopic = collector.statsForTopic("test-topic", true);
    System.out.println(statsForTopic);
    assertNotNull(statsForTopic);

    assertTrue(statsForTopic.contains("prod-test-topic--rate-per-sec.produce rate-per-sec:"));

  }

  @Test
  public void shouldHandleConcurrency() throws Exception {

    ExecutorService executorService = Executors.newFixedThreadPool(4);

    ArrayList<ProducerCollector> collectors = new ArrayList<>();

    Metrics metrics = new Metrics();
    collectors.add(new ProducerCollector(metrics, "stream-thread-1"));
    collectors.add(new ProducerCollector(metrics, "stream-thread-2"));
    collectors.add(new ProducerCollector(metrics, "stream-thread-3"));
    collectors.add(new ProducerCollector(metrics, "stream-thread-4"));

    for (int i = 0; i < 1000; i++){

      final int index = i % collectors.size();
      executorService.submit(() -> collectors.get(index).onSend(new ProducerRecord(TEST_TOPIC, 1, "key", "value")));
    }

    executorService.shutdown();
    executorService.awaitTermination(10, TimeUnit.SECONDS);

    String statsForTopic = collectors.get(0).statsForTopic(TEST_TOPIC, true);
    assertNotNull(statsForTopic);

    assertTrue("totalevents is wrong:" + statsForTopic, statsForTopic.contains("total-events:   1000.00"));
  }

}