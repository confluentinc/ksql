package io.confluent.ksql.internal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.KsqlConfig;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StorageUtilizationMetricsReporterTest {

  private static final String KAFKA_METRIC_NAME = "total-sst-files-size";
  private static final String KAFKA_METRIC_GROUP = "streams-metric";
  private static final String KSQL_METRIC_GROUP = "ksqldb_utilization";
  private static final String THREAD_ID_SUFFIX = "3d62ddb9-d520-4cb3-9c23-968f8e61e201-StreamThread-1";
  private static final String THREAD_ID = "_confluent_blahblah_query_CTAS_TEST_1-" + THREAD_ID_SUFFIX;
  private static final String TRANSIENT_THREAD_ID = "_confluent_blahblah_transient_blahblah_4-" + THREAD_ID_SUFFIX;
  private static final String TASK_STORAGE_METRIC = "task_storage_used_bytes";
  private static final String QUERY_STORAGE_METRIC = "query_storage_used_bytes";
  private static final Map<String, String> BASE_TAGS = ImmutableMap.of("logical_cluster_id", "logical-id");
  private static final Map<String, String> QUERY_TAGS = ImmutableMap.of("logical_cluster_id", "logical-id", "query-id", "CTAS_TEST_1");
  private static final Map<String, String> TASK_ONE_TAGS = ImmutableMap.of("logical_cluster_id", "logical-id", "query-id", "CTAS_TEST_1", "task-id", "t1");
  private static final Map<String, String> TASK_TWO_TAGS = ImmutableMap.of("logical_cluster_id", "logical-id", "query-id", "CTAS_TEST_1", "task-id", "t2");

  private StorageUtilizationMetricsReporter listener;

  @Mock
  private Metrics metrics;
  @Captor
  private ArgumentCaptor<MetricValueProvider<?>> metricValueProvider;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    listener = new StorageUtilizationMetricsReporter();
    listener.configure(
        ImmutableMap.of(
            KsqlConfig.KSQL_INTERNAL_METRICS_CONFIG, metrics
        )
    );
    when(metrics.metricName(any(), any(), any(Map.class))).thenAnswer(
      a -> new MetricName(a.getArgument(0), a.getArgument(1), "", a.getArgument(2)));
    StorageUtilizationMetricsReporter.setTags(BASE_TAGS);
  }

  @Test
  @SuppressFBWarnings({
    "BX_UNBOXING_IMMEDIATELY_REBOXED", "" +
    "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE"
  })
  public void shouldAddNodeMetricsOnConfigure() throws IOException {
    // Given:
    listener.metricChange(mockMetric(
        KAFKA_METRIC_GROUP,
        KAFKA_METRIC_NAME,
        BigInteger.valueOf(2),
        ImmutableMap.of("task-id", "t1", "thread-id", THREAD_ID))
    );
    final File f = new File("/tmp/storage-test/");
    f.getParentFile().mkdirs();
    f.createNewFile();
    StorageUtilizationMetricsReporter.configureShared(f, metrics, BASE_TAGS);

    // When:
    final Gauge<?> storageFreeGauge = verifyAndGetRegisteredMetric("node_storage_free_bytes", BASE_TAGS);
    final Object storageFreeValue = storageFreeGauge.value(null, 0);
    final Gauge<?> storageTotalGauge = verifyAndGetRegisteredMetric("node_storage_total_bytes", BASE_TAGS);
    final Object storageTotalValue = storageTotalGauge.value(null, 0);
    final Gauge<?> storageUsedGauge = verifyAndGetRegisteredMetric("node_storage_used_bytes", BASE_TAGS);
    final Object storageUsedValue = storageUsedGauge.value(null, 0);
    final Gauge<?> pctUsedGauge = verifyAndGetRegisteredMetric("storage_utilization", BASE_TAGS);
    final Object pctUsedValue = pctUsedGauge.value(null, 0);
    final Gauge<?> maxTaskUsageGauge = verifyAndGetRegisteredMetric("max_used_task_storage_bytes", BASE_TAGS);
    final Object maxTaskUsageValue = maxTaskUsageGauge.value(null, 0);
    final Gauge<?> numStatefulTasksGauge = verifyAndGetRegisteredMetric("num_stateful_tasks", BASE_TAGS);
    final Object numStatefulTasksValue = numStatefulTasksGauge.value(null, 0);
    
    // Then:
    assertThat((Long) storageFreeValue, greaterThan(0L));
    assertThat((Long) storageTotalValue, greaterThan(0L));
    assertThat((Long) storageUsedValue, greaterThan(0L));
    assertThat((Double) pctUsedValue, greaterThan(0.0));
    assertThat((Integer) numStatefulTasksValue, greaterThan(0));
    assertThat((BigInteger) maxTaskUsageValue, greaterThanOrEqualTo(BigInteger.ZERO));
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

    final Gauge<?> queryGauge = verifyAndGetRegisteredMetric(QUERY_STORAGE_METRIC, QUERY_TAGS);
    final Object queryValue = queryGauge.value(null, 0);
    final Gauge<?> taskGauge = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, TASK_ONE_TAGS);
    final Object taskValue = taskGauge.value(null, 0);

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
      ImmutableMap.of("task-id", "t1", "thread-id", THREAD_ID, "logical_cluster_id", "logical-id"))
    );

    // When:
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(15),
      ImmutableMap.of("task-id", "t1", "thread-id", THREAD_ID, "logical_cluster_id", "logical-id"))
    );

    // Then:
    final Gauge<?> taskGauge = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, TASK_ONE_TAGS);
    final Object taskValue = taskGauge.value(null, 0);
    final Gauge<?> queryGauge = verifyAndGetRegisteredMetric(QUERY_STORAGE_METRIC, QUERY_TAGS);
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
      ImmutableMap.of("task-id", "t1", "thread-id", THREAD_ID, "logical_cluster_id", "logical-id"))
    );
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(5),
      ImmutableMap.of("task-id", "t2", "thread-id", THREAD_ID, "logical_cluster_id", "logical-id"))
    );

    // Then:
    final Gauge<?> queryGauge = verifyAndGetRegisteredMetric(QUERY_STORAGE_METRIC, QUERY_TAGS);
    final Object queryValue = queryGauge.value(null, 0);
    final Gauge<?> taskGaugeOne = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, TASK_ONE_TAGS);
    final Object taskValueOne = taskGaugeOne.value(null, 0);
    final Gauge<?> taskGaugeTwo = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, TASK_TWO_TAGS);
    final Object taskValueTwo = taskGaugeTwo.value(null, 0);


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
    final Gauge<?> taskGauge = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, ImmutableMap.of("task-id", "t1", "query-id", "blahblah_4", "logical_cluster_id", "logical-id"));
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
    verifyRemovedMetric(QUERY_STORAGE_METRIC, QUERY_TAGS);
    verifyRemovedMetric(TASK_STORAGE_METRIC, TASK_ONE_TAGS);

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
    final Gauge<?> taskGauge = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, TASK_ONE_TAGS);
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
    assertThrows(AssertionError.class, () -> verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, TASK_ONE_TAGS));
  }

  @Test
  public void shouldCombineTaskMetricsToQueryMetricWithSharedRuntimeQueries() {
    // When:
    listener.metricChange(mockMetric(
        KAFKA_METRIC_GROUP,
        KAFKA_METRIC_NAME,
        BigInteger.valueOf(2),
        ImmutableMap.of("task-id", "CTAS_TEST_1__1_0", "thread-id", "THREAD_ID", "logical_cluster_id", "logical-id"))
    );
    listener.metricChange(mockMetric(
        KAFKA_METRIC_GROUP,
        KAFKA_METRIC_NAME,
        BigInteger.valueOf(5),
        ImmutableMap.of("task-id", "CTAS_TEST_1__1_1", "thread-id", "THREAD_ID", "logical_cluster_id", "logical-id"))
    );

    // Then:
    final Gauge<?> queryGauge = verifyAndGetRegisteredMetric(QUERY_STORAGE_METRIC, QUERY_TAGS);
    final Object queryValue = queryGauge.value(null, 0);
    final Map<String, String> task1 = ImmutableMap.of("logical_cluster_id", "logical-id", "query-id", "CTAS_TEST_1", "task-id", "CTAS_TEST_1__1_0");
    final Gauge<?> taskGaugeOne = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, task1);
    final Object taskValueOne = taskGaugeOne.value(null, 0);
    final Map<String, String> task2 = ImmutableMap.of("logical_cluster_id", "logical-id", "query-id", "CTAS_TEST_1", "task-id", "CTAS_TEST_1__1_1");
    final Gauge<?> taskGaugeTwo = verifyAndGetRegisteredMetric(TASK_STORAGE_METRIC, task2);
    final Object taskValueTwo = taskGaugeTwo.value(null, 0);
    
    assertThat(taskValueOne, equalTo(BigInteger.valueOf(2)));
    assertThat(taskValueTwo, equalTo(BigInteger.valueOf(5)));
    assertThat(queryValue, equalTo(BigInteger.valueOf(7)));
  }
  
  @Test
  public void shouldRecordMaxTaskUsage() {
    // Given:
    KafkaMetric m1 = mockMetric(
      KSQL_METRIC_GROUP,
      TASK_STORAGE_METRIC,
      BigInteger.valueOf(2),
      ImmutableMap.of("task-id", "t1"));
    KafkaMetric m2 = mockMetric(
      KSQL_METRIC_GROUP,
      TASK_STORAGE_METRIC,
      BigInteger.valueOf(5),
      ImmutableMap.of("task-id", "t2"));
    MetricName n1 = m1.metricName();
    MetricName n2 = m2.metricName();
    when(metrics.metrics()).thenReturn(ImmutableMap.of(n1, m1, n2, m2));
    
    // When:
    listener.metricChange(m1);
    listener.metricChange(m2);
    
    // Then:
    BigInteger maxVal = StorageUtilizationMetricsReporter.getMaxTaskUsage(metrics);
    assertEquals(maxVal, BigInteger.valueOf(5));
  }

  @Test
  public void shouldRecordMaxTaskUsageWithNoTasks() {
    // Given:
    when(metrics.metrics()).thenReturn(Collections.emptyMap());

    // When:

    // Then:
    BigInteger maxVal = StorageUtilizationMetricsReporter.getMaxTaskUsage(metrics);
    assertEquals(maxVal, BigInteger.valueOf(0));
  }
  
  @Test
  public void shouldRecordNumStatefulTasks() {
    // Given:
    final File f = new File("/tmp/storage-test/");
    StorageUtilizationMetricsReporter.configureShared(f, metrics, BASE_TAGS);
    final Gauge<?> numStatefulTasksGauge = verifyAndGetRegisteredMetric("num_stateful_tasks", BASE_TAGS);

    // When:
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(2),
      ImmutableMap.of("store-id", "s1", "task-id", "t2", "thread-id", TRANSIENT_THREAD_ID))
    );
    listener.metricChange(mockMetric(
      KAFKA_METRIC_GROUP,
      KAFKA_METRIC_NAME,
      BigInteger.valueOf(5),
      ImmutableMap.of("store-id", "s2", "task-id", "t1", "thread-id", TRANSIENT_THREAD_ID))
    );
    
    // Then:
    final Object numStatefulTasksValue = numStatefulTasksGauge.value(null, 0);
    assertEquals((int) numStatefulTasksValue, 2);
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
        n -> n.name().equals(name) && n.tags().entrySet().equals(tags.entrySet())
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
