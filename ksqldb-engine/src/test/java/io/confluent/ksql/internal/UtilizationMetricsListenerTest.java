package io.confluent.ksql.internal;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@RunWith(MockitoJUnitRunner.class)
public class UtilizationMetricsListenerTest {

  UtilizationMetricsListener listener;
  List<KafkaStreams> streamsList;
  List<String> metricNames;
  @Before
  public void setUp() {
    streamsList = new ArrayList<>();
    final Topology topology = new Topology();
    final Properties config = new Properties();
    final TopologyTestDriver driver = new TopologyTestDriver(topology, config);
    final StreamsBuilder builder = new StreamsBuilder();
    builder.<String, String>stream("test-topic")
            .flatMapValues(value -> Collections.singletonList(""));
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "blahblah");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamsList.add(new KafkaStreams(builder.build(), config));
    streamsList.add(new KafkaStreams(builder.build(), config));
    streamsList.add(new KafkaStreams(builder.build(), config));
    streamsList.add(new KafkaStreams(builder.build(), config));
    streamsList.add(new KafkaStreams(builder.build(), config));
    for (KafkaStreams stream : streamsList) {
      stream.start();
    }
    listener = new UtilizationMetricsListener(100L, streamsList);

    metricNames = new ArrayList<>();
    metricNames.add("poll-time-total");
    metricNames.add("restore-consumer-poll-time-total");
    metricNames.add("send-time-total");
    metricNames.add("flush-time-total");
  }

  @Test
  public void shouldHaveProcessingRatio() throws InterruptedException {
    assertThat(listener.processingRatio(), greaterThan(0.0));
    wait(3000);
    assertThat(listener.processingRatio(), greaterThan(0.0));
  }

  @Test
  public void shouldHaveAllProcessingRatioMetrics() {
    final long windowEnd = System.currentTimeMillis();
    final long windowStart = (long) Math.max(0, windowEnd - 100.0);
    for (KafkaStreams stream : streamsList) {
      for (ThreadMetadata thread : stream.localThreadsMetadata()) {
        listener.getProcessingRatio(thread.threadName(), stream, windowStart, 100L);
        //assertThat(listener.temporaryThreadMetrics.size(), is(4));
        System.out.println(listener.temporaryThreadMetrics.keySet());
        //assertThat(listener.temporaryThreadMetrics.keySet(), containsInAnyOrder(metricNames));
      }
    }
  }
}
