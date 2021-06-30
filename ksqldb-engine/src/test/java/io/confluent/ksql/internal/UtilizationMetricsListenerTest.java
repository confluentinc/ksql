package io.confluent.ksql.internal;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.ThreadMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UtilizationMetricsListenerTest {

  UtilizationMetricsListener listener;
  List<KafkaStreams> streamsList;
  List<String> metricNames;

  @Mock
  private KafkaStreams s1;
  @Mock
  private KafkaMetric ioWaitTime;
  @Mock
  private KafkaMetric ioTime;
  @Mock
  private KafkaMetric flush;
  @Mock
  private KafkaMetric bufferpool;
  @Mock
  private KafkaMetric startTime;
  @Mock
  private KafkaMetric ioWaitTimeRC;
  @Mock
  private KafkaMetric ioTimeRC;
  @Mock
  private KafkaMetric ioWaitTime_1;
  @Mock
  private KafkaMetric ioTime_1;
  @Mock
  private KafkaMetric flush_1;
  @Mock
  private KafkaMetric bufferpool_1;
  @Mock
  private KafkaMetric startTime_1;
  @Mock
  private KafkaMetric ioWaitTime_2;
  @Mock
  private KafkaMetric ioWaitTimeRC_1;
  @Mock
  private KafkaMetric ioTimeRC_1;
  @Mock
  private ThreadMetadata s1_t1;
  @Mock
  private ThreadMetadata s1_t2;
  @Mock
  private Time time;

  @Before
  public void setUp() {
    streamsList = new ArrayList<>();
    streamsList.add(s1);

    listener = new UtilizationMetricsListener(streamsList, time, 200L);

    metricNames = new ArrayList<>();
    metricNames.add("io-waittime-total");
    metricNames.add("iotime-total");
    metricNames.add("bufferpool-wait-time-total");
    metricNames.add("flush-time-total");

    when(time.milliseconds()).thenReturn(500L);

    when(s1_t1.threadName()).thenReturn("s1_t1");
    when(s1_t2.threadName()).thenReturn("s1_t2");

    when(s1.metadataForLocalThreads()).thenReturn(ImmutableSet.of(s1_t1, s1_t2));

    when(ioWaitTime.metricName()).thenReturn(new MetricName("io-waittime-total", "consumer-metrics", "", ImmutableMap.of("client-id", "s1_t1")));
    when(ioWaitTimeRC.metricName()).thenReturn(new MetricName("io-waittime-total", "consumer-metrics", "", ImmutableMap.of("client-id", "s1_t1_restore-consumer")));
    when(ioTime.metricName()).thenReturn(new MetricName("iotime-total", "consumer-metrics", "", ImmutableMap.of("client-id", "s1_t1")));
    when(ioTimeRC.metricName()).thenReturn(new MetricName("iotime-total", "consumer-metrics", "", ImmutableMap.of("client-id", "s1_t1_restore-consumer")));
    when(bufferpool.metricName()).thenReturn(new MetricName("bufferpool-wait-time-total", "producer-metrics", "", ImmutableMap.of("client-id", "s1_t1")));
    when(flush.metricName()).thenReturn(new MetricName("flush-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t1")));
    when(startTime.metricName()).thenReturn(new MetricName("thread-start-time", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t1")));

    when(ioWaitTime_1.metricName()).thenReturn(new MetricName("io-waittime-total", "consumer-metrics", "", ImmutableMap.of("client-id", "s1_t2")));
    when(ioWaitTimeRC_1.metricName()).thenReturn(new MetricName("io-waittime-total", "consumer-metrics", "", ImmutableMap.of("client-id", "s1_t2_restore-consumer")));
    when(ioTime_1.metricName()).thenReturn(new MetricName("iotime-total", "consumer-metrics", "", ImmutableMap.of("client-id", "s1_t2")));
    when(ioTimeRC_1.metricName()).thenReturn(new MetricName("iotime-total", "consumer-metrics", "", ImmutableMap.of("client-id", "s1_t2_restore-consumer")));
    when(bufferpool_1.metricName()).thenReturn(new MetricName("bufferpool-wait-time-total", "producer-metrics", "", ImmutableMap.of("client-id", "s1_t2")));
    when(flush_1.metricName()).thenReturn(new MetricName("flush-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t2")));
    when(startTime_1.metricName()).thenReturn(new MetricName("thread-start-time", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t2")));
  }

  @Test
  public void shouldGetMinOfAllThreads() {
    doReturn(createMetrics("s1_t1", "s1_t2")).when(s1).metrics();

    // All threads started before window
    // Thread 1 -> blocked for 65 / 300
    when(ioWaitTime.metricValue()).thenReturn(10000000.0);
    when(ioWaitTimeRC.metricValue()).thenReturn(10000000.0);
    when(ioTime.metricValue()).thenReturn(10000000.0);
    when(ioTimeRC.metricValue()).thenReturn(5000000.0);
    when(flush.metricValue()).thenReturn(10.0);
    when(bufferpool.metricValue()).thenReturn(20000000.0);
    when(startTime.metricValue()).thenReturn(200L);

    // thread 2 -> blocked for 9
    when(ioWaitTime_1.metricValue()).thenReturn(3000000.0);
    when(ioWaitTimeRC_1.metricValue()).thenReturn(2000000.0);
    when(ioTime_1.metricValue()).thenReturn(1000000.0);
    when(ioTimeRC_1.metricValue()).thenReturn(0000000.0);
    when(flush_1.metricValue()).thenReturn(1.0);
    when(bufferpool_1.metricValue()).thenReturn(2000000.0);
    when(startTime_1.metricValue()).thenReturn(200L);

    assertThat(listener.processingRatio(), equalTo(97.0));
  }

  @Test
  public void shouldHandleOverlyLargeBlockedTime() {
    doReturn(createMetrics("s1_t1", "s1_t2")).when(s1).metrics();

    // thread 1 -> blocked for 751.5 / 300
    when(ioWaitTime.metricValue()).thenReturn(300000000.0);
    when(ioWaitTimeRC.metricValue()).thenReturn(200000000.0);
    when(ioTime.metricValue()).thenReturn(100000000.0);
    when(ioTimeRC.metricValue()).thenReturn(1000000.0);
    when(flush.metricValue()).thenReturn(130.0);
    when(bufferpool.metricValue()).thenReturn(20500000.0);
    when(startTime.metricValue()).thenReturn(200L);

    // thread 2 -> blocked for 150 / 300
    when(ioWaitTime_1.metricValue()).thenReturn(30000000.0);
    when(ioWaitTimeRC_1.metricValue()).thenReturn(20000000.0);
    when(ioTime_1.metricValue()).thenReturn(50000000.0);
    when(ioTimeRC_1.metricValue()).thenReturn(50000000.0);
    when(flush_1.metricValue()).thenReturn(0.0);
    when(bufferpool_1.metricValue()).thenReturn(0.0);
    when(startTime_1.metricValue()).thenReturn(200L);

    assertThat(listener.processingRatio(), equalTo(50.0));
  }

  @Test
  public void shouldProperlyComputeWithDifferentStartTimes() {
    doReturn(createMetrics("s1_t1", "s1_t2")).when(s1).metrics();

    // window start is 200L
    // Thread 1 -> blocked for 165 / 300
    when(ioWaitTime.metricValue()).thenReturn(10000000.0);
    when(ioWaitTimeRC.metricValue()).thenReturn(10000000.0);
    when(ioTime.metricValue()).thenReturn(10000000.0);
    when(ioTimeRC.metricValue()).thenReturn(5000000.0);
    when(flush.metricValue()).thenReturn(100.0);
    when(bufferpool.metricValue()).thenReturn(30000000.0);
    when(startTime.metricValue()).thenReturn(100L);

    // thread 2 -> blocked for 55 / 300 -> obvious min but since started within
    // the window, should not get this value for blocked time
    when(ioWaitTimeRC_1.metricValue()).thenReturn(5000000.0);
    when(ioWaitTime_1.metricValue()).thenReturn(5000000.0);
    when(ioTimeRC_1.metricValue()).thenReturn(10000000.0);
    when(ioTime_1.metricValue()).thenReturn(2000000.0);
    when(flush_1.metricValue()).thenReturn(13.0);
    when(bufferpool_1.metricValue()).thenReturn(20000000.0);
    when(startTime_1.metricValue()).thenReturn(350L);

    assertThat(listener.processingRatio(), equalTo(45.0));

  }

  private Map<MetricName, KafkaMetric> createMetrics(final String threadOne, final String threadTwo) {
    final Map<MetricName, KafkaMetric> metricMap = new HashMap<>();
    metricMap.put(new MetricName("io-waittime-total", "consumer-metrics", "", ImmutableMap.of("client-id", threadOne)), ioWaitTime);
    metricMap.put(new MetricName("io-waittime-total", "consumer-metrics", "", ImmutableMap.of("client-id", threadOne + "_restore-consumer")), ioWaitTimeRC);
    metricMap.put(new MetricName("iotime-total", "consumer-metrics", "", ImmutableMap.of("client-id", threadOne)), ioTime);
    metricMap.put(new MetricName("iotime-total", "consumer-metrics", "", ImmutableMap.of("client-id", threadOne + "_restore-consumer")), ioTimeRC);
    metricMap.put(new MetricName("bufferpool-wait-time-total", "stream-thread-metrics", "", ImmutableMap.of("client-id", threadOne)), bufferpool);
    metricMap.put(new MetricName("flush-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadOne)), flush);
    metricMap.put(new MetricName("thread-start-time", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadOne)), startTime);


    metricMap.put(new MetricName("io-waittime-total", "consumer-metrics", "", ImmutableMap.of("client-id", threadTwo)), ioWaitTime_1);
    metricMap.put(new MetricName("io-waittime-total", "consumer-metrics", "", ImmutableMap.of("client-id", threadTwo + "_restore-consumer")), ioWaitTimeRC_1);
    metricMap.put(new MetricName("iotime-total", "consumer-metrics", "", ImmutableMap.of("client-id", threadTwo)), ioTime_1);
    metricMap.put(new MetricName("iotime-total", "consumer-metrics", "", ImmutableMap.of("client-id", threadTwo + "_restore-consumer")), ioTimeRC_1);
    metricMap.put(new MetricName("bufferpool-wait-time-total", "stream-thread-metrics", "", ImmutableMap.of("client-id", threadTwo)), bufferpool_1);
    metricMap.put(new MetricName("flush-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadTwo)), flush_1);
    metricMap.put(new MetricName("thread-start-time", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadTwo)), startTime_1);

    return metricMap;
  }
}
