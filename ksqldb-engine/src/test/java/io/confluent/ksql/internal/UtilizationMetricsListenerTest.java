package io.confluent.ksql.internal;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.ThreadMetadata;
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
import static org.hamcrest.Matchers.greaterThan;
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
  private KafkaMetric poll;
  @Mock
  private KafkaMetric restoreConsumer;
  @Mock
  private KafkaMetric flush;
  @Mock
  private KafkaMetric send;
  @Mock
  private KafkaMetric startTime;
  @Mock
  private KafkaMetric poll_1;
  @Mock
  private KafkaMetric restoreConsumer_1;
  @Mock
  private KafkaMetric flush_1;
  @Mock
  private KafkaMetric send_1;
  @Mock
  private KafkaMetric startTime_1;
  @Mock
  private KafkaMetric poll_2;
  @Mock
  private KafkaMetric restoreConsumer_2;
  @Mock
  private KafkaMetric flush_2;
  @Mock
  private KafkaMetric send_2;
  @Mock
  private KafkaMetric startTime_2;
  @Mock
  private ThreadMetadata s1_t1;
  @Mock
  private ThreadMetadata s1_t2;
  @Mock
  private ThreadMetadata s1_t3;

  @Before
  public void setUp() {
    streamsList = new ArrayList<>();
    streamsList.add(s1);

    listener = new UtilizationMetricsListener(700L, streamsList);

    metricNames = new ArrayList<>();
    metricNames.add("poll-time-total");
    metricNames.add("restore-consumer-poll-time-total");
    metricNames.add("send-time-total");
    metricNames.add("flush-time-total");

    when(s1_t1.threadName()).thenReturn("s1_t1");
    when(s1_t2.threadName()).thenReturn("s1_t2");
    when(s1_t3.threadName()).thenReturn("s1_t3");

    when(s1.localThreadsMetadata()).thenReturn(ImmutableSet.of(s1_t1, s1_t2, s1_t3));

    when(poll.metricName()).thenReturn(new MetricName("poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t1")));
    when(restoreConsumer.metricName()).thenReturn(new MetricName("restore-consumer-poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t1")));
    when(send.metricName()).thenReturn(new MetricName("send-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t1")));
    when(flush.metricName()).thenReturn(new MetricName("flush-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t1")));
    when(startTime.metricName()).thenReturn(new MetricName("thread-start-time", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t1")));

    when(poll_1.metricName()).thenReturn(new MetricName("poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t2")));
    when(restoreConsumer_1.metricName()).thenReturn(new MetricName("restore-consumer-poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t2")));
    when(send_1.metricName()).thenReturn(new MetricName("send-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t2")));
    when(flush_1.metricName()).thenReturn(new MetricName("flush-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t2")));
    when(startTime_1.metricName()).thenReturn(new MetricName("thread-start-time", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t2")));

    when(poll_2.metricName()).thenReturn(new MetricName("poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t3")));
    when(restoreConsumer_2.metricName()).thenReturn(new MetricName("restore-consumer-poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t3")));
    when(send_2.metricName()).thenReturn(new MetricName("send-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t3")));
    when(flush_2.metricName()).thenReturn(new MetricName("flush-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t3")));
    when(startTime_2.metricName()).thenReturn(new MetricName("thread-start-time", "stream-thread-metrics", "", ImmutableMap.of("thread-id", "s1_t3")));
  }

  @Test
  public void shouldGetMinOfAllThreads() {
    doReturn(createMetrics("s1_t1", "s1_t2", "s1_t3")).when(s1).metrics();

    final long threadStartTime = System.currentTimeMillis() - 200L;
    // Thread 1 -> blocked for 65 / 700
    when(poll.metricValue()).thenReturn(20.0);
    when(restoreConsumer.metricValue()).thenReturn(15.0);
    when(flush.metricValue()).thenReturn(10.0);
    when(send.metricValue()).thenReturn(20.0);
    when(startTime.metricValue()).thenReturn(threadStartTime);

    when(poll_1.metricValue()).thenReturn(50.0);
    when(restoreConsumer_1.metricValue()).thenReturn(11.0);
    when(flush_1.metricValue()).thenReturn(13.0);
    when(send_1.metricValue()).thenReturn(25.0);
    when(startTime_1.metricValue()).thenReturn(threadStartTime);

    // thread 3 -> blocked for 9
    when(poll_2.metricValue()).thenReturn(5.0);
    when(restoreConsumer_2.metricValue()).thenReturn(1.0);
    when(flush_2.metricValue()).thenReturn(1.0);
    when(send_2.metricValue()).thenReturn(2.0);
    when(startTime_2.metricValue()).thenReturn(threadStartTime);

    assertThat(listener.processingRatio(), equalTo(99.0));
  }

  @Test
  public void shouldHandleMissingThreadData() {
    doReturn(createMetrics("s1_t1", "s1_t2", "")).when(s1).metrics();

    final long threadStartTime = System.currentTimeMillis() - 200L;
    // Thread 1 -> blocked for 65 / 700
    when(poll.metricValue()).thenReturn(20.0);
    when(restoreConsumer.metricValue()).thenReturn(15.0);
    when(flush.metricValue()).thenReturn(10.0);
    when(send.metricValue()).thenReturn(20.0);
    when(startTime.metricValue()).thenReturn(threadStartTime);

    // thread 2 -> blocked for 99 / 700
    when(poll_1.metricValue()).thenReturn(50.0);
    when(restoreConsumer_1.metricValue()).thenReturn(11.0);
    when(flush_1.metricValue()).thenReturn(13.0);
    when(send_1.metricValue()).thenReturn(25.0);
    when(startTime_1.metricValue()).thenReturn(threadStartTime);

    // I don't know if this is really right, if the thread is missing data it returns 0 for processing time
    // which will always be the minimum
    // thread 3 -> missing

    assertThat(listener.processingRatio(), equalTo(100.0));
  }

  @Test
  public void shouldHandleOverlyLargeBlockedTime() {
    doReturn(createMetrics("s1_t1", "s1_t2", "s1_t3")).when(s1).metrics();

    final long threadStartTime = System.currentTimeMillis() - 200L;
    // Thread 1 -> blocked for 750 / 700
    when(poll.metricValue()).thenReturn(200.0);
    when(restoreConsumer.metricValue()).thenReturn(150.0);
    when(flush.metricValue()).thenReturn(100.0);
    when(send.metricValue()).thenReturn(300.0);
    when(startTime.metricValue()).thenReturn(threadStartTime);

    // thread 2 -> blocked for 945 / 700
    when(poll_1.metricValue()).thenReturn(500.0);
    when(restoreConsumer_1.metricValue()).thenReturn(110.0);
    when(flush_1.metricValue()).thenReturn(130.0);
    when(send_1.metricValue()).thenReturn(205.0);
    when(startTime_1.metricValue()).thenReturn(threadStartTime);

    // thread 3 -> blocked for 150 / 700
    when(poll_2.metricValue()).thenReturn(50.0);
    when(restoreConsumer_2.metricValue()).thenReturn(100.0);
    when(flush_2.metricValue()).thenReturn(0.0);
    when(send_2.metricValue()).thenReturn(0.0);
    when(startTime_2.metricValue()).thenReturn(threadStartTime);

    // hard to predict the value because the `blockedTime` is based on sample time which we can't really predict
    assertThat(listener.processingRatio(), greaterThan(78.0));
  }

  private Map<MetricName, KafkaMetric> createMetrics(final String threadOne, final String threadTwo, final String threadThree) {
    final Map<MetricName, KafkaMetric> metricMap = new HashMap<>();
    metricMap.put(new MetricName("poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadOne)), poll);
    metricMap.put(new MetricName("restore-consumer-poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadOne)), restoreConsumer);
    metricMap.put(new MetricName("send-time-total", "-tream-thread-metrics", "", ImmutableMap.of("thread-id", threadOne)), send);
    metricMap.put(new MetricName("flush-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadOne)), flush);
    metricMap.put(new MetricName("thread-start-time", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadOne)), startTime);


    metricMap.put(new MetricName("poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadTwo)), poll_1);
    metricMap.put(new MetricName("restore-consumer-poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadTwo)), restoreConsumer_1);
    metricMap.put(new MetricName("send-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadTwo)), send_1);
    metricMap.put(new MetricName("flush-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadTwo)), flush_1);
    metricMap.put(new MetricName("thread-start-time", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadTwo)), startTime_1);

    if (!threadThree.equals("")) {
      metricMap.put(new MetricName("poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadThree)), poll_2);
      metricMap.put(new MetricName("restore-consumer-poll-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadThree)), restoreConsumer_2);
      metricMap.put(new MetricName("send-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadThree)), send_2);
      metricMap.put(new MetricName("flush-time-total", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadThree)), flush_2);
      metricMap.put(new MetricName("thread-start-time", "stream-thread-metrics", "", ImmutableMap.of("thread-id", threadThree)), startTime_2);
    }
    return metricMap;
  }
}
