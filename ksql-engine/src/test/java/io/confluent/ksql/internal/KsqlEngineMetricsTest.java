/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.confluent.ksql.internal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class KsqlEngineMetricsTest {

  private static final String METRIC_GROUP = "testGroup";
  private KsqlEngineMetrics engineMetrics;
  private static final String KSQL_SERVICE_ID = "test-ksql-service-id";
  private static final String metricNamePrefix = KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX + KSQL_SERVICE_ID;

  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private QueryMetadata query1;

  @Before
  public void setUp() {
    MetricCollectors.initialize();
    when(ksqlEngine.getServiceId()).thenReturn(KSQL_SERVICE_ID);
    when(query1.getQueryApplicationId()).thenReturn("app-1");

    engineMetrics = new KsqlEngineMetrics(METRIC_GROUP, ksqlEngine, MetricCollectors.getMetrics());
  }

  @After
  public void tearDown() {
    engineMetrics.close();
    MetricCollectors.cleanUp();
  }

  @Test
  public void shouldRemoveAllSensorsOnClose() {
    assertTrue(engineMetrics.registeredSensors().size() > 0);

    engineMetrics.close();

    engineMetrics.registeredSensors().forEach(sensor -> {
      assertThat(engineMetrics.getMetrics().getSensor(sensor.name()), is(nullValue()));
    });
  }

  @Test
  public void shouldRecordNumberOfActiveQueries() {
    when(ksqlEngine.numberOfLiveQueries()).thenReturn(3);
    final double value = getMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "num-active-queries");
    assertEquals(3.0, value, 0.0);
  }

  @Test
  public void shouldRecordNumberOfQueriesInCREATEDState() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(3, State.CREATED));

    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-CREATED-queries");
    assertEquals(3L, value);
  }

  @Test
  public void shouldRecordNumberOfQueriesInRUNNINGState() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(3, State.RUNNING));

    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-RUNNING-queries");
    assertEquals(3L, value);
  }

  @Test
  public void shouldRecordNumberOfQueriesInREBALANCINGState() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(3, State.REBALANCING));

    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-REBALANCING-queries");
    assertEquals(3L, value);
  }

  @Test
  public void shouldRecordNumberOfQueriesInPENDING_SHUTDOWNGState() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(3, State.PENDING_SHUTDOWN));

    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-PENDING_SHUTDOWN-queries");
    assertEquals(3L, value);
  }

  @Test
  public void shouldRecordNumberOfQueriesInERRORState() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(3, State.ERROR));

    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-ERROR-queries");
    assertEquals(3L, value);
  }

  @Test
  public void shouldRecordNumberOfQueriesInNOT_RUNNINGtate() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(4, State.NOT_RUNNING));

    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-NOT_RUNNING-queries");
    assertEquals(4L, value);
  }

  @Test
  public void shouldRecordNumberOfPersistentQueries() {
    when(ksqlEngine.numberOfPersistentQueries()).thenReturn(3);

    final double value = getMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "num-persistent-queries");
    assertEquals(3.0, value, 0.0);
  }

  @Test
  public void shouldRecordMessagesConsumed() {
    final int numMessagesConsumed = 500;
    consumeMessages(numMessagesConsumed, "group1");
    engineMetrics.updateMetrics();
    final double value = getMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "messages-consumed-per-sec");
    assertEquals(numMessagesConsumed / 100, Math.floor(value), 0.01);
  }

  @Test
  public void shouldRecordMessagesProduced() {
    final int numMessagesProduced = 500;
    produceMessages(numMessagesProduced);
    engineMetrics.updateMetrics();
    final double value = getMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "messages-produced-per-sec");
    assertEquals(numMessagesProduced / 100, Math.floor(value), 0.01);
  }

  @Test
  public void shouldRecordMessagesConsumedByQuery() {
    final int numMessagesConsumed = 500;
    consumeMessages(numMessagesConsumed, "group1");
    consumeMessages(numMessagesConsumed * 100, "group2");
    engineMetrics.updateMetrics();
    final double maxValue = getMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "messages-consumed-max");
    assertEquals(numMessagesConsumed, Math.floor(maxValue), 5.0);
    final double minValue = getMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "messages-consumed-min");
    assertEquals(numMessagesConsumed / 100, Math.floor(minValue), 0.01);
  }

  @Test
  public void shouldRegisterQueries() {
    // When:
    engineMetrics.registerQuery(query1);

    // Then:
    verify(query1).registerQueryStateListener(any());
  }

  private static double getMetricValue(final Metrics metrics, final String metricName) {
    return Double.valueOf(
        metrics.metric(metrics.metricName(metricName, METRIC_GROUP + "-query-stats"))
            .metricValue().toString());
  }

  private static long getLongMetricValue(final Metrics metrics, final String metricName) {
    return Long.parseLong(
        metrics.metric(metrics.metricName(metricName, METRIC_GROUP + "-query-stats"))
            .metricValue().toString());
  }

  private static void consumeMessages(final int numMessages, final String groupId) {
    final ConsumerCollector collector1 = new ConsumerCollector();
    collector1.configure(ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, groupId));
    final Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = new HashMap<>();
    final List<ConsumerRecord<Object, Object>> recordList = new ArrayList<>();
    for (int i = 0; i < numMessages; i++) {
      recordList.add(new ConsumerRecord<>("foo", 1, 1, 1L, TimestampType
          .CREATE_TIME, 1L, 10, 10, "key", "1234567890"));
    }
    records.put(new TopicPartition("foo", 1), recordList);
    final ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);
    collector1.onConsume(consumerRecords);
  }

  private static void produceMessages(final int numMessages) {
    final ProducerCollector collector1 = new ProducerCollector();
    collector1.configure(ImmutableMap.of(ProducerConfig.CLIENT_ID_CONFIG, "client1"));
    for (int i = 0; i < numMessages; i++) {
      collector1.onSend(new ProducerRecord<>("foo", "key", Integer.toString(i)));
    }
  }

  private static Answer<List<PersistentQueryMetadata>> returnQueriesInState(
      final int numberOfQueries,
      final KafkaStreams.State state
  ) {
    return invocation -> {
      final List<PersistentQueryMetadata> queryMetadataList = new ArrayList<>();
      for (int i = 0; i < numberOfQueries; i++) {
        final PersistentQueryMetadata query = mock(PersistentQueryMetadata.class);
        when(query.getState()).thenReturn(state.toString());
        queryMetadataList.add(query);
      }
      return queryMetadataList;
    };
  }
}
