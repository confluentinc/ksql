package io.confluent.ksql.internal;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StorageUtilizationMetricsReporterTest {

  private static final String KAFKA_METRIC_NAME = "total-sst-files-size";
  private static final String KAFKA_METRIC_GROUP = "streams-metric";
  private static final String THREAD_ID = "_confluent_blahblah_query_CTAS_TEST_1-blahblah";
  private static final String TRANSIENT_THREAD_ID = "_confluent_blahblah_transient_blahblah_4-blahblah";
  private static final String TASK_STORAGE_METRIC = "task_storage_used_bytes";
  private static final String QUERY_STORAGE_METRIC = "query_storage_used_bytes";

  private StorageUtilizationMetricsReporter listener;

  @Mock
  private Metrics metrics;
  @Captor
  private ArgumentCaptor<MetricValueProvider<?>> metricValueProvider;

  @Before
  public void setUp() {
    listener = new StorageUtilizationMetricsReporter(metrics);
    when(metrics.metricName(any(), any(), (Map<String, String>) any())).thenAnswer(
      a -> new MetricName(a.getArgument(0), a.getArgument(1), "", a.getArgument(2)));
    when(metrics.metricName(any(), any())).thenAnswer(
      a -> new MetricName(a.getArgument(0), a.getArgument(1), "", Collections.emptyMap()));
    listener.configure(ImmutableMap.of("state.dir", "/tmp/cat/"));
  }

  @After
  public void cleanup() {
    StorageUtilizationMetricsReporter.reset();
  }

  @Test
  public void shouldAddNodeMetricsOnConfigure() {
    // Given:

    // When:
    final Gauge<?> storageAvailableGauge = verifyAndGetRegisteredMetric("node_storage_available_bytes", Collections.emptyMap());
    final Object storageAvailableValue = storageAvailableGauge.value(null, 0);
    final Gauge<?> storageTotalGauge = verifyAndGetRegisteredMetric("node_storage_total_bytes", Collections.emptyMap());
    final Object storageTotalValue = storageTotalGauge.value(null, 0);
    final Gauge<?> storageUsedGauge = verifyAndGetRegisteredMetric("node_storage_used_bytes", Collections.emptyMap());
    final Object storageUsedValue = storageUsedGauge.value(null, 0);

    // Then:
    assertThat(storageAvailableValue, equalTo(0L));
    assertThat(storageTotalValue, equalTo(0L));
    assertThat(storageUsedValue, equalTo(0L));
  }

  @Test
  public void shouldOnlyAddNodeMetricsOnce() {
    // Given:

    // When:
    listener.configure(ImmutableMap.of("state.dir", "/tmp/cat/"));

    // Then:
    // did not throw error for duplicate metrics
  }

  @Test
  public void shouldAddNewGauges() {
    // Given:
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(2),
      ImmutableMap.of("task-id", "t1", "thread-id", THREAD_ID))
    );

    // When:
    final Gauge<?> taskGauge = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, ImmutableMap.of("task-id", "t1", "query-id", "CTAS_TEST_1"));
    final Object taskValue = taskGauge.value(null, 0);
    final Gauge<?> queryGauge = verifyAndGetRegisteredMetric(QUERY_STORAGE_METRIC, ImmutableMap.of("query-id", "CTAS_TEST_1"));
    final Object queryValue = queryGauge.value(null, 0);

    // Then:
    assertThat(taskValue, equalTo(BigInteger.valueOf(2)));
    assertThat(queryValue, equalTo(BigInteger.valueOf(2)));

  }

  @Test
  public void shouldUpdateExistingGauges() {
    // Given:
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(2),
      ImmutableMap.of("task-id", "t1", "thread-id", THREAD_ID))
    );

    // When:
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(15),
      ImmutableMap.of("task-id", "t1", "thread-id", THREAD_ID))
    );

    // Then:
    final Gauge<?> taskGauge = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, ImmutableMap.of("task-id", "t1", "query-id", "CTAS_TEST_1"));
    final Object taskValue = taskGauge.value(null, 0);
    final Gauge<?> queryGauge = verifyAndGetRegisteredMetric(QUERY_STORAGE_METRIC, ImmutableMap.of("query-id", "CTAS_TEST_1"));
    final Object queryValue = queryGauge.value(null, 0);

    assertThat(taskValue, equalTo(BigInteger.valueOf(15)));
    assertThat(queryValue, equalTo(BigInteger.valueOf(15)));
  }

  @Test
  public void shouldCombineTaskMetricsToQueryMetric() {
    // When:
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(2),
      ImmutableMap.of("task-id", "t1", "thread-id", THREAD_ID))
    );
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(5),
      ImmutableMap.of("task-id", "t2", "thread-id", THREAD_ID))
    );

    // Then:
    final Gauge<?> taskGaugeOne = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, ImmutableMap.of("task-id", "t1", "query-id", "CTAS_TEST_1"));
    final Object taskValueOne = taskGaugeOne.value(null, 0);
    final Gauge<?> taskGaugeTwo = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, ImmutableMap.of("task-id", "t2", "query-id", "CTAS_TEST_1"));
    final Object taskValueTwo = taskGaugeTwo.value(null, 0);
    final Gauge<?> queryGauge = verifyAndGetRegisteredMetric(QUERY_STORAGE_METRIC, ImmutableMap.of("query-id", "CTAS_TEST_1"));
    final Object queryValue = queryGauge.value(null, 0);

    assertThat(taskValueOne, equalTo(BigInteger.valueOf(2)));
    assertThat(taskValueTwo, equalTo(BigInteger.valueOf(5)));
    assertThat(queryValue, equalTo(BigInteger.valueOf(7)));
  }

  @Test
  public void shouldCombineStorageMetricsToTaskMetric() {
    // When:
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(2),
      ImmutableMap.of("store-id", "s1", "task-id", "t1", "thread-id", TRANSIENT_THREAD_ID))
    );
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(5),
      ImmutableMap.of("store-id", "s2", "task-id", "t1", "thread-id", TRANSIENT_THREAD_ID))
    );

    // Then:
    final Gauge<?> taskGauge = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, ImmutableMap.of("task-id", "t1", "query-id", "blahblah_4"));
    final Object taskValue = taskGauge.value(null, 0);
    assertThat(taskValue, equalTo(BigInteger.valueOf(7)));
  }

  @Test
  public void shouldRemoveTaskAndQueryGauges() {
    // Given:
    final KafkaMetric metric = mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(2),
      ImmutableMap.of("task-id", "t1", "thread-id", THREAD_ID));
    listener.metricChange(metric);

    // When:
    listener.metricRemoval(metric);

    // Then:
    verifyRemovedMetric(TASK_STORAGE_METRIC, ImmutableMap.of("task-id", "t1", "query-id", "CTAS_TEST_1"));
    verifyRemovedMetric(QUERY_STORAGE_METRIC, ImmutableMap.of("query-id", "CTAS_TEST_1"));

  }

  @Test
  public void shouldRemoveObsoleteStateStoreMetrics() {
    // Given:
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(2),
      ImmutableMap.of("store-id", "s1", "task-id", "t1", "thread-id", THREAD_ID)));
    final KafkaMetric metric = mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(6),
      ImmutableMap.of("store-id", "s2", "task-id", "t1", "thread-id", THREAD_ID));
    listener.metricChange(metric);
    final Gauge<?> taskGauge = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, ImmutableMap.of("task-id", "t1", "query-id", "CTAS_TEST_1"));
    Object taskValue = taskGauge.value(null, 0);
    assertThat(taskValue, equalTo(BigInteger.valueOf(8)));

    // When:
    listener.metricRemoval(metric);

    // Then:
    taskValue = taskGauge.value(null, 0);
    assertThat(taskValue, equalTo(BigInteger.valueOf(2)));
  }

  @Test
  public void shouldIgnoreNonSSTMetrics() {
    // When:
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      "other-metric",
      BigInteger.valueOf(2),
      ImmutableMap.of("store-id", "s1", "task-id", "t1", "thread-id", THREAD_ID)));

    // Then:
    assertThrows(AssertionError.class, () -> verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, ImmutableMap.of("task-id", "t1", "query-id", "CTAS_TEST_1")));
  }

  private KafkaMetric mockMetric(
    final String group, final String name, Object value, final Map<String, String> tags) {
    final KafkaMetric metric = mock(KafkaMetric.class);
    when(metric.metricName()).thenReturn(
      new MetricName(name, group, "", tags));
    when(metric.metricValue()).thenReturn(value);
    return metric;
  }

  private Gauge<?> verifyAndGetRegisteredMetric(final String name, final Map<String, String> tags) {
    verify(metrics).addMetric(
      argThat(
        n -> n.name().equals(name) && n.tags().equals(tags)
      ),
      metricValueProvider.capture()
    );
    return (Gauge<?>) metricValueProvider.getValue();
  }

  private void verifyRemovedMetric(final String name, final Map<String, String> tags) {
    verify(metrics).removeMetric(
      argThat(
        n -> n.name().equals(name) && n.tags().equals(tags)
      )
    );
  }
}
