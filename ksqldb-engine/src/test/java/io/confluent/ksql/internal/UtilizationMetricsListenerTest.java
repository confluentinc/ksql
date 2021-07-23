package io.confluent.ksql.internal;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UtilizationMetricsListenerTest {

  List<File> directories;
  ConcurrentHashMap<QueryId, KafkaStreams> streams;
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
  private KafkaMetric sst_2;
  @Mock
  private File file;
  @Captor
  private ArgumentCaptor<List<MetricsReporter.DataPoint>> dataPointCaptor;

  @Before
  public void setUp() {
    when(query.getQueryApplicationId()).thenReturn("app-id");
    when(query.getQueryId()).thenReturn(queryId);
    when(query.getKafkaStreams()).thenReturn(s1);

    directories = new ArrayList<>();
    streams = new ConcurrentHashMap<>();
    streams.put(new QueryId("blah"), s1);
    listener = new UtilizationMetricsListener(streams, directories, new File("tmp/cat/"));
  }

  @Test
  public void shouldAddStateStoreOnCreation() {
    // When:
    listener.onCreate(serviceContext, metaStore, query);

    // Then:
    assertTrue(directories.contains(new File("tmp/cat/app-id")));
  }

  @Test
  public void shouldRemoveStateStoreOnTermination() {
    // When:
    listener.onCreate(serviceContext, metaStore, query);
    listener.onDeregister(query);

    // Then:
    assertTrue(directories.isEmpty());
  }

  @Test
  public void shouldReportNodeLevelStorageUsage() {
    // Given:
    directories.add(file);
    when(file.getFreeSpace()).thenReturn(50L);
    when(file.getTotalSpace()).thenReturn(100L);
    when(file.getName()).thenReturn("/tmp/cat/new_query");

    // When:
    ArrayList<MetricsReporter.DataPoint> dataPoints = new ArrayList<>();
    listener.nodeDiskUsage(dataPoints, Instant.EPOCH);

    // Then
    assertEquals("Should record storage usage", dataPoints.get(0), new MetricsReporter.DataPoint(Instant.EPOCH, "storage-usage", 50.0));
    assertEquals("Should record storage percentage", dataPoints.get(1), new MetricsReporter.DataPoint(Instant.EPOCH, "storage-usage-perc", 50.0));
    assertEquals(dataPoints.size(),2);

  }

  @Test
  public void shouldReportTaskLevelStorageUsage() {
    // Given:
    directories.add(file);
    final MetricName metricName_1 = new MetricName("total-sst-files-size", "", "", ImmutableMap.of("task-id", "t1"));
    final MetricName metricName_2 = new MetricName("total-sst-files-size", "", "", ImmutableMap.of("task-id", "t2"));
    doReturn(ImmutableMap.of(metricName_1, sst_1, metricName_2, sst_2)).when(s1).metrics();
    when(sst_1.metricValue()).thenReturn(BigInteger.valueOf(20L));
    when(sst_1.metricName()).thenReturn(metricName_1);
    when(sst_2.metricValue()).thenReturn(BigInteger.valueOf(10L));
    when(sst_2.metricName()).thenReturn(metricName_2);

    // When:
    ArrayList<MetricsReporter.DataPoint> dataPoints = new ArrayList<>();
    listener.taskDiskUsage(dataPoints, Instant.EPOCH);

    // Then:
    assertEquals(dataPoints.get(0), new MetricsReporter.DataPoint(
      Instant.EPOCH,
      "task-storage-usage",
      20.0,
      ImmutableMap.of("task-id", "t1", "query-id", "blah")));
    assertEquals(dataPoints.get(1), new MetricsReporter.DataPoint(
      Instant.EPOCH,
      "task-storage-usage",
      10.0,
      ImmutableMap.of("task-id", "t2", "query-id", "blah")));
    assertEquals(dataPoints.get(2), new MetricsReporter.DataPoint(
      Instant.EPOCH,
      "query-storage-usage",
      30.0,
      ImmutableMap.of("query-id", "blah")));
    assertEquals(dataPoints.size(),3);
  }
}
