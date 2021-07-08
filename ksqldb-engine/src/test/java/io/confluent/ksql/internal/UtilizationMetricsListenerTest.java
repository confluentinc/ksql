package io.confluent.ksql.internal;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.logging.query.TestAppender;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UtilizationMetricsListenerTest {

  List<File> directories;
  HashMap<QueryId, KafkaStreams> streams;
  private UtilizationMetricsListener listener;

  @Mock
  private QueryMetadata query;
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

  @Before
  public void setUp() {
    when(query.getQueryApplicationId()).thenReturn("app-id");

    directories = new ArrayList<>();
    streams = new HashMap<>();
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
}
