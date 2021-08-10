package io.confluent.ksql.internal;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UtilizationMetricsListenerTest {

  ConcurrentHashMap<String, Long> streams;
  private UtilizationMetricsListener listener;

  @Mock
  private QueryMetadata query;
  @Mock
  private QueryId queryId;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private MetaStore metaStore;
  @Mock
  private KafkaStreams s1;
  @Mock
  private KafkaMetric sst_1;
  @Mock
  private Metrics metricsRegistry;

  @Before
  public void setUp() {
    when(query.getQueryId()).thenReturn(queryId);
    when(query.getKafkaStreams()).thenReturn(s1);

    streams = new ConcurrentHashMap<>();
    listener = new UtilizationMetricsListener(metricsRegistry, new HashMap<>());
  }

  /*@Test
  public void shouldAddKafkaStreamsOnCreation() {
    // When:
    listener.onCreate(serviceContext, metaStore, query);

    // Then:
    assertEquals(1, streams.size());
  }

  @Test
  public void shouldRemoveKafkaStreamsOnTermination() {
    // When:
    listener.onCreate(serviceContext, metaStore, query);
    listener.onDeregister(query);

    // Then:
    assertTrue(streams.isEmpty());
  }

  @Test
  public void shouldAddNodeLevelMetricsOnCreation() {
    // When:
    listener.onCreate(serviceContext, metaStore, query);

    // Then
    verify(metricsRegistry).metricName(
      "node-storage-available",
      "ksql-utilization-metrics");
    verify(metricsRegistry).metricName(
      "node-storage-total",
      "ksql-utilization-metrics");
    verify(metricsRegistry).metricName(
      "node-storage-used",
      "ksql-utilization-metrics");
    verify(metricsRegistry).addMetric(
      eq(metricsRegistry.metricName(
      "node-storage-available",
      "ksql-utilization-metrics")),
      isA(Gauge.class)
    );
    verify(metricsRegistry).addMetric(
      eq(metricsRegistry.metricName(
        "node-storage-total",
        "ksql-utilization-metrics")),
      isA(Gauge.class)
    );
    verify(metricsRegistry).addMetric(
      eq(metricsRegistry.metricName(
        "node-storage-used",
        "ksql-utilization-metrics")),
      isA(Gauge.class)
    );
  }

  @Test
  public void shouldRegisterTaskLevelMetrics() {

  }

  @Test
  public void shouldRemoveObsoleteTaskMetrics() {
    // Given:
    final MetricName removableMetric = new MetricName("name", "group", "description", Collections.emptyMap());
    final Set<UtilizationMetricsListener.TaskStorageMetric> temporaryMetrics = new HashSet<>();
    temporaryMetrics.add(new UtilizationMetricsListener.TaskStorageMetric(removableMetric));
    final UtilizationMetricsListener metricsListener =
      new UtilizationMetricsListener(
        streams,
        new File("tmp/cat/"),
        metricsRegistry,
        temporaryMetrics
      );
    streams.put(new QueryId("q1"), s1);
    when(s1.metrics()).thenReturn(Collections.emptyMap());

    // When:
    metricsListener.taskDiskUsage();

    // Then:
    verify(metricsRegistry).removeMetric(removableMetric);
  }

  @Test
  public void shouldUpdateTaskStorageValues() {
    // Given:
    final MetricName sst_metric = new MetricName("total-sst-files-size", "", "", ImmutableMap.of("task-id", "t1"));
    final UtilizationMetricsListener.TaskStorageMetric firstMetric =
      new UtilizationMetricsListener.TaskStorageMetric(
        new MetricName(
          "query-storage-usage",
          "ksql-utilization-metrics",
          "",
          ImmutableMap.of("task-id", "t1", "query-id", "q1")));
    firstMetric.updateValue(50L);
    final Set<UtilizationMetricsListener.TaskStorageMetric> temporaryMetrics = new HashSet<>();
    temporaryMetrics.add(firstMetric);

    streams.put(new QueryId("q1"), s1);
    doReturn(ImmutableMap.of(sst_metric, sst_1)).when(s1).metrics();
    when(sst_1.metricValue()).thenReturn(BigInteger.valueOf(50L));
    when(sst_1.metricName()).thenReturn(sst_metric);

    final UtilizationMetricsListener metricsListener =
      new UtilizationMetricsListener(
        streams,
        new File("tmp/cat/"),
        metricsRegistry,
        temporaryMetrics
      );

    // When:
    metricsListener.taskDiskUsage();

    // Then:
    assertEquals(100L, firstMetric.value);

  }*/

}
