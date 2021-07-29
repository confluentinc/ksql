package io.confluent.ksql.internal;

import com.google.common.collect.ImmutableMap;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UtilizationMetricsListenerTest {

  List<File> directories;
  ConcurrentHashMap<QueryId, KafkaStreams> streams;
  private UtilizationMetricsListener listener;

  private static final MetricName NODE_METRIC_NAME = new MetricName(
    "node-storage-usage",
    "ksql-utilization-metrics",
    "d1",
    ImmutableMap.of("query-id", "q1"));

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
  private KafkaMetric sst_2;
  @Mock
  private File file;
  @Mock
  private Metrics metricsRegistry;
  @Captor
  private ArgumentCaptor<List<MetricsReporter.DataPoint>> dataPointCaptor;

  @Before
  public void setUp() {
    when(query.getQueryApplicationId()).thenReturn("app-id");
    when(query.getQueryId()).thenReturn(queryId);
    when(query.getKafkaStreams()).thenReturn(s1);

    when(metricsRegistry.metricName(any(), any(), any(), anyMap()))
      .thenReturn(NODE_METRIC_NAME);

    streams = new ConcurrentHashMap<>();
    //streams.put(new QueryId("blah"), s1);
    listener = new UtilizationMetricsListener(streams, new File("tmp/cat/"), metricsRegistry);
  }

  @Test
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
  public void shouldAddNodeLevelMetricOnCreation() {
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

}
