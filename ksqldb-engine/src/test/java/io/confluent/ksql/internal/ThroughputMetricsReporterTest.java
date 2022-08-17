package io.confluent.ksql.internal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThroughputMetricsReporterTest {
  private static final String RECORDS_CONSUMED_TOTAL = "records-consumed-total";
  private static final String BYTES_CONSUMED_TOTAL = "bytes-consumed-total";
  private static final String RECORDS_PRODUCED_TOTAL = "records-produced-total";
  private static final String BYTES_PRODUCED_TOTAL = "bytes-produced-total";
  private static final String QUERY_ID = "CTAS_TEST";
  private static final String THREAD_ID_SUFFIX = "3d62ddb9-d520-4cb3-9c23-968f8e61e201-StreamThread-1";
  private static final String THREAD_ID = "_confluent_blahblah_query_CTAS_TEST_1-" + THREAD_ID_SUFFIX;
  private static final String THREAD_ID_2 = "_confluent_blahblah_query_CTAS_TEST_2-" + THREAD_ID_SUFFIX;
  private static final String TRANSIENT_THREAD_ID = "_confluent_blahblah_transient_blahblah_4-" + THREAD_ID_SUFFIX;
  private static final String TASK_ID_1 = "0_1";
  private static final String TASK_ID_2 = "0_2";
  private static final String PROCESSOR_NODE_ID = "sink-node";
  private static final String PROCESSOR_NODE_ID_2 = "sink-node-2";
  private static final String TOPIC_NAME = "topic";
  private static final String TOPIC_NAME_2 = "topic-2";

  private static final Map<String, String> STREAMS_TAGS_TASK_1 = ImmutableMap.of(
      "thread-id", THREAD_ID,
      "task-id", TASK_ID_1,
      "processor-node-id", PROCESSOR_NODE_ID,
      "topic", TOPIC_NAME
  );
  private static final Map<String, String> STREAMS_TAGS_TASK_2 = ImmutableMap.of(
      "thread-id", THREAD_ID,
      "task-id", TASK_ID_2,
      "processor-node-id", PROCESSOR_NODE_ID,
      "topic", TOPIC_NAME
  );
  private static final Map<String, String> STREAMS_TAGS_PROCESSOR_2 = ImmutableMap.of(
      "thread-id", THREAD_ID,
      "task-id", TASK_ID_2,
      "processor-node-id", PROCESSOR_NODE_ID_2,
      "topic", TOPIC_NAME
  );
  private static final Map<String, String> STREAMS_TAGS_TOPIC_2 = ImmutableMap.of(
      "thread-id", THREAD_ID_2,
      "task-id", TASK_ID_1,
      "processor-node-id", PROCESSOR_NODE_ID,
      "topic", TOPIC_NAME_2
  );
  private static final Map<String, String> QUERY_ONE_TAGS = ImmutableMap.of(
      "logical_cluster_id", "lksqlc-12345",
      "query-id", QUERY_ID + "_1",
      "member", THREAD_ID,
      "topic", TOPIC_NAME
  );
  private static final Map<String, String> QUERY_TWO_TAGS = ImmutableMap.of(
      "logical_cluster_id", "lksqlc-12345",
      "query-id", QUERY_ID + "_2",
      "member", THREAD_ID_2,
      "topic", TOPIC_NAME_2
  );

  private ThroughputMetricsReporter listener;

  @Mock
  private Metrics metrics;
  @Captor
  private ArgumentCaptor<Measurable> metricValueProvider;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    listener = new ThroughputMetricsReporter();
    listener.configure(
        ImmutableMap.of(
            KsqlConfig.KSQL_INTERNAL_METRICS_CONFIG, metrics,
            KsqlConfig.KSQL_CUSTOM_METRICS_TAGS, "logical_cluster_id:lksqlc-12345"
        )
    );
  }

  @Before
  public void cleanUp() {
    ThroughputMetricsReporter.reset();
  }
    
  @Test
  public void shouldAddNewMeasurableForAllThroughputMetricsTypes() {
    // Given:
    listener.metricChange(mockMetric(
        BYTES_CONSUMED_TOTAL,
        1D,
        STREAMS_TAGS_TASK_1)
    );

    listener.metricChange(mockMetric(
        RECORDS_CONSUMED_TOTAL,
        2D,
        STREAMS_TAGS_TASK_1)
    );

    listener.metricChange(mockMetric(
        BYTES_PRODUCED_TOTAL,
        3D,
        STREAMS_TAGS_TASK_1)
    );

    listener.metricChange(mockMetric(
        RECORDS_PRODUCED_TOTAL,
        4D,
        STREAMS_TAGS_TASK_1)
    );

    // When:
    final Measurable bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, QUERY_ONE_TAGS);
    final Measurable recordsConsumed = verifyAndGetMetric(RECORDS_CONSUMED_TOTAL, QUERY_ONE_TAGS);
    final Measurable bytesProduced = verifyAndGetMetric(BYTES_PRODUCED_TOTAL, QUERY_ONE_TAGS);
    final Measurable recordsProduced = verifyAndGetMetric(RECORDS_PRODUCED_TOTAL, QUERY_ONE_TAGS);

    // Then:
    assertThat(bytesConsumed.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0L), equalTo(1D));
    assertThat(recordsConsumed.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0L), equalTo(2D));
    assertThat(bytesProduced.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0L), equalTo(3D));
    assertThat(recordsProduced.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0L), equalTo(4D));
  }

  @Test
  public void shouldUpdateExistingMeasurable() {
    // Given:
    listener.metricChange(mockMetric(
        BYTES_CONSUMED_TOTAL,
        2D,
        STREAMS_TAGS_TASK_1)
    );

    Measurable bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, QUERY_ONE_TAGS);
    Object bytesConsumedValue = bytesConsumed.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0L);
    assertThat(bytesConsumedValue, equalTo(2D));

    // When:
    listener.metricChange(mockMetric(
        BYTES_CONSUMED_TOTAL,
        15D,
        STREAMS_TAGS_TASK_1)
    );

    // Then:
    bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, QUERY_ONE_TAGS);
    bytesConsumedValue = bytesConsumed.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0L);

    assertThat(bytesConsumedValue, equalTo(15D));
  }

  @Test
  public void shouldAggregateOverAllTasksAndProcessorNodes() {
    // Given:
    KafkaMetric m1 = mockMetric(
        BYTES_CONSUMED_TOTAL,
        2D,
        STREAMS_TAGS_TASK_1
    );
    KafkaMetric m2 = mockMetric(
        BYTES_CONSUMED_TOTAL,
        5D,
        STREAMS_TAGS_TASK_2
    );
    KafkaMetric m3 = mockMetric(
        BYTES_CONSUMED_TOTAL,
        3D,
        STREAMS_TAGS_PROCESSOR_2
    );

    // When:
    listener.metricChange(m1);
    listener.metricChange(m2);
    listener.metricChange(m3);

    // Then:
    final Measurable bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, QUERY_ONE_TAGS);
    final Object value = bytesConsumed.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0L);
    assertThat(value, equalTo(10D));
  }

  @Test
  public void shouldRemoveCorrectMetricAndReturnZero() {
    // Given:
    final KafkaMetric metric = mockMetric(
        BYTES_CONSUMED_TOTAL,
        2D,
        STREAMS_TAGS_TASK_1);
    listener.metricChange(metric);
    final Measurable bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, QUERY_ONE_TAGS);

    final KafkaMetric metric2 = mockMetric(
        BYTES_CONSUMED_TOTAL,
        5D,
        STREAMS_TAGS_TOPIC_2);
    listener.metricChange(metric2);
    final Measurable bytesConsumed2 = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, QUERY_TWO_TAGS);

    Object value = bytesConsumed.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0L);
    assertThat(value, equalTo(2D));
    Object value2 = bytesConsumed2.measure(new MetricConfig().tags(QUERY_TWO_TAGS), 0L);
    assertThat(value2, equalTo(5D));

    // When:
    listener.metricRemoval(metric);

    // Then:
    verifyRemovedMetric(BYTES_CONSUMED_TOTAL, QUERY_ONE_TAGS);

    value = bytesConsumed.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0L);
    assertThat(value, equalTo(0D));

    value2 = bytesConsumed2.measure(new MetricConfig().tags(QUERY_TWO_TAGS), 0L);
    assertThat(value2, equalTo(5D));
  }

  @Test
  public void shouldNotIncludeRemovedMetricInThroughputTotal() {
    // Given:
    listener.metricChange(mockMetric(
        BYTES_CONSUMED_TOTAL,
        2D,
        STREAMS_TAGS_TASK_1)
    );
    final KafkaMetric metric2 = mockMetric(
        BYTES_CONSUMED_TOTAL,
        6D,
        STREAMS_TAGS_TASK_2
    );
    listener.metricChange(metric2);
    final Measurable bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, QUERY_ONE_TAGS);
    Object taskValue = bytesConsumed.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0L);
    assertThat(taskValue, equalTo(8D));

    // When:
    listener.metricRemoval(metric2);

    // Then:
    taskValue = bytesConsumed.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0);
    assertThat(taskValue, equalTo(2D));

    final KafkaMetric metric3 = mockMetric(
        BYTES_CONSUMED_TOTAL,
        3D,
        STREAMS_TAGS_PROCESSOR_2);
    listener.metricChange(metric3);
    taskValue = bytesConsumed.measure(new MetricConfig().tags(QUERY_ONE_TAGS), 0);
    assertThat(taskValue, equalTo(5D));
  }

  @Test
  public void shouldAggregateMetricsByQueryIdForTransientQueries() {
    // Given:
    final Map<String, String> transientQueryTags = ImmutableMap.of(
        "logical_cluster_id", "lksqlc-12345",
        "query-id", "blahblah_4",
        "member", TRANSIENT_THREAD_ID,
        "topic", TOPIC_NAME
    );
    listener.metricChange(mockMetric(
        BYTES_CONSUMED_TOTAL,
        2D,
        ImmutableMap.of(
            "thread-id", TRANSIENT_THREAD_ID,
            "task-id", TASK_ID_1,
            "processor-node-id", PROCESSOR_NODE_ID,
            "topic", TOPIC_NAME))
    );

    Measurable bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, transientQueryTags);
    Object bytesConsumedValue =
      bytesConsumed.measure(new MetricConfig().tags(transientQueryTags), 0L);
    assertThat(bytesConsumedValue, equalTo(2D));

    // When:
    listener.metricChange(mockMetric(
      BYTES_CONSUMED_TOTAL,
      15D,
      ImmutableMap.of(
        "thread-id", TRANSIENT_THREAD_ID,
        "task-id", TASK_ID_2,
        "processor-node-id", PROCESSOR_NODE_ID,
        "topic", TOPIC_NAME
      ))
    );

    // Then:
    bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, transientQueryTags);
    bytesConsumedValue = bytesConsumed.measure(new MetricConfig().tags(transientQueryTags), 0L);

    assertThat(bytesConsumedValue, equalTo(17D));
  }

  @Test
  public void shouldAggregateMetricsByQueryIdInSharedRuntimes() {
    // Given:
    final Map<String, String> sharedRuntimeQueryTags = ImmutableMap.of(
      "logical_cluster_id", "lksqlc-12345",
      "query-id", "CTAS_TEST_5",
      "member", "_confluent_blahblah_query-1-blahblah",
      "topic", TOPIC_NAME
    );
    listener.metricChange(mockMetric(
      BYTES_CONSUMED_TOTAL,
      2D,
      ImmutableMap.of(
        "thread-id", "_confluent_blahblah_query-1-blahblah",
        "task-id", "CTAS_TEST_5__" + TASK_ID_1,
        "processor-node-id", PROCESSOR_NODE_ID,
        "topic", TOPIC_NAME))
    );

    Measurable bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, sharedRuntimeQueryTags);
    Object bytesConsumedValue =
      bytesConsumed.measure(new MetricConfig().tags(sharedRuntimeQueryTags), 0L);
    assertThat(bytesConsumedValue, equalTo(2D));

    // When:
    listener.metricChange(mockMetric(
      BYTES_CONSUMED_TOTAL,
      15D,
      ImmutableMap.of(
        "thread-id", "_confluent_blahblah_query-1-blahblah",
        "task-id", "CTAS_TEST_5__" + TASK_ID_2,
        "processor-node-id", PROCESSOR_NODE_ID,
        "topic", TOPIC_NAME
      ))
    );

    // Then:
    bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, sharedRuntimeQueryTags);
    bytesConsumedValue = bytesConsumed.measure(new MetricConfig().tags(sharedRuntimeQueryTags), 0L);

    assertThat(bytesConsumedValue, equalTo(17D));
  }

  @Test
  public void shouldExtractQueryIdWithHyphenInSharedRuntimes() {
    // Given:
    final Map<String, String> sharedRuntimeQueryTags = ImmutableMap.of(
        "logical_cluster_id", "lksqlc-12345",
        "query-id", "CSAS_TEST_COPY-STREAM_1_23",
        "member", "_confluent_blahblah_query-1-blahblah",
        "topic", TOPIC_NAME
    );
    listener.metricChange(mockMetric(
        BYTES_CONSUMED_TOTAL,
        2D,
        ImmutableMap.of(
            "thread-id", "_confluent_blahblah_query-1-blahblah",
            "task-id", "CSAS_TEST_COPY-STREAM_1_23__" + TASK_ID_1,
            "processor-node-id", PROCESSOR_NODE_ID,
            "topic", TOPIC_NAME))
    );

    Measurable bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, sharedRuntimeQueryTags);
    Object bytesConsumedValue =
        bytesConsumed.measure(new MetricConfig().tags(sharedRuntimeQueryTags), 0L);
    assertThat(bytesConsumedValue, equalTo(2D));
  }

  @Test
  public void shouldExtractQueryIdWithHyphenInUnsharedRuntimes() {
    // Given:
    final String threadId =
        "_confluent-ksql-pksqlc-d1m0zquery_"                           // thread prefix
            + "CSAS_TEST_COPY-STREAM_1_23"                             // query id
            + "-3d62ddb9-d520-4cb3-9c23-968f8e61e201-StreamThread-1";  // thread    suffix
    final Map<String, String> sharedRuntimeQueryTags = ImmutableMap.of(
        "logical_cluster_id", "lksqlc-12345",
        "query-id", "CSAS_TEST_COPY-STREAM_1_23",
        "member", threadId,
        "topic", TOPIC_NAME
    );
    listener.metricChange(mockMetric(
        BYTES_CONSUMED_TOTAL,
        2D,
        ImmutableMap.of(
            "thread-id", threadId,
            "task-id", TASK_ID_1,
            "processor-node-id", PROCESSOR_NODE_ID,
            "topic", TOPIC_NAME))
    );

    Measurable bytesConsumed = verifyAndGetMetric(BYTES_CONSUMED_TOTAL, sharedRuntimeQueryTags);
    Object bytesConsumedValue =
        bytesConsumed.measure(new MetricConfig().tags(sharedRuntimeQueryTags), 0L);
    assertThat(bytesConsumedValue, equalTo(2D));
  }

  @Test
  public void shouldIgnoreNonThroughputMetric() {
    // When:
    listener.metricChange(mockMetric(
      "other-metric",
      2D,
      STREAMS_TAGS_TASK_1)
    );

    // Then:
    assertThrows(AssertionError.class, () -> verifyAndGetMetric("other-metric", QUERY_ONE_TAGS));
  }

  @Test
  public void shouldThrowWhenFailingToParseQueryId() {
    // When:
    assertThrows(
      KsqlException.class,
      () -> listener.metricChange(mockMetric(
        BYTES_CONSUMED_TOTAL,
        2D,
        ImmutableMap.of(
          "thread-id", "_confluent_blahblah_query-blahblah",
          "task-id", TASK_ID_1,
          "processor-node-id", PROCESSOR_NODE_ID,
          "topic", TOPIC_NAME))
      )
    );
  }

  @Test
  public void shouldThrowWhenTopicNameTagIsMissing() {
    // When:
    assertThrows(
      KsqlException.class,
      () -> listener.metricChange(mockMetric(
        BYTES_CONSUMED_TOTAL,
        2D,
        ImmutableMap.of(
          "thread-id", THREAD_ID,
          "task-id", TASK_ID_1,
          "processor-node-id", PROCESSOR_NODE_ID))
      )
    );
  }

  private KafkaMetric mockMetric(
    final String name, 
    Object value, 
    final Map<String, String> tags
  ) {
    final KafkaMetric metric = mock(KafkaMetric.class);
    when(metric.metricName()).thenReturn(
      new MetricName(name, "stream-topic-metrics", "", tags));
    when(metric.metricValue()).thenReturn(value);
    return metric;
  }

  private Measurable verifyAndGetMetric(final String name, final Map<String, String> tags) {
    verify(metrics);
    metrics.addMetric(
      argThat(
        n -> n.name().equals(name)
            && n.tags().entrySet().equals(tags.entrySet())
      ),
      metricValueProvider.capture()
    );
    return metricValueProvider.getValue();
  }

  private void verifyRemovedMetric(final String name, final Map<String, String> tags) {
    verify(metrics).removeMetric(
      argThat(
        n -> n.name().equals(name) && n.tags().equals(tags)
      )
    );
  }
}
