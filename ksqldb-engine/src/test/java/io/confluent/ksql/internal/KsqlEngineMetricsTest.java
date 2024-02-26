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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
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
  private static final String KSQL_SERVICE_ID = "test-ksql-service-id";
  private static final String legacyMetricNamePrefix = ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX
      + KSQL_SERVICE_ID;
  private static final String metricNamePrefix = ReservedInternalTopics.CONFLUENT_PREFIX;
  private static final Map<String, String> CUSTOM_TAGS = ImmutableMap.of("tag1", "value1", "tag2", "value2");
  private static final Map<String, String> CUSTOM_TAGS_WITH_SERVICE_ID = ImmutableMap.of(
      "tag1", "value1",
          "tag2", "value2",
          KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, KSQL_SERVICE_ID);

  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private QueryMetadata query1;

  private KsqlEngineMetrics engineMetrics;
  private MetricCollectors metricCollectors;

  @Before
  public void setUp() {
    when(ksqlEngine.getServiceId()).thenReturn(KSQL_SERVICE_ID);

    metricCollectors = new MetricCollectors();
    engineMetrics = new KsqlEngineMetrics(
        METRIC_GROUP,
        ksqlEngine,
        metricCollectors,
        CUSTOM_TAGS,
        Optional.of(new TestKsqlMetricsExtension()));
  }

  @After
  public void tearDown() {
    engineMetrics.close();
  }

  @Test
  public void shouldRemoveAllSensorsOnClose() {
    assertTrue(engineMetrics.registeredSensors().size() > 0);

    engineMetrics.close();

    engineMetrics.registeredSensors().forEach(sensor -> assertThat(engineMetrics.getMetrics().getSensor(sensor.name()), is(nullValue())));
  }

  @Test
  public void shouldRecordLivenessIndicator() {
    final double value = getMetricValue("liveness-indicator");
    final double legacyValue = getMetricValueLegacy("liveness-indicator");

    assertThat(value, equalTo(1.0));
    assertThat(legacyValue, equalTo(1.0));
  }

  @Test
  public void shouldRecordNumberOfActiveQueries() {
    when(ksqlEngine.numberOfLiveQueries()).thenReturn(3);

    final double value = getMetricValue("num-active-queries");
    final double legacyValue = getMetricValueLegacy("num-active-queries");

    assertThat(value, equalTo(3.0));
    assertThat(legacyValue, equalTo(3.0));
  }

  @Test
  public void shouldRecordNumberOfQueriesInCREATEDState() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(3, State.CREATED));

    final long value = getLongMetricValue("CREATED-queries");
    final long legacyValue = getLongMetricValueLegacy("CREATED-queries");

    assertThat(value, equalTo(3L));
    assertThat(legacyValue, equalTo(3L));
  }

  @Test
  public void shouldRecordNumberOfQueriesInRUNNINGState() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(3, State.RUNNING));

    final long value = getLongMetricValue("RUNNING-queries");
    final long legacyValue = getLongMetricValueLegacy("RUNNING-queries");

    assertThat(value, equalTo(3L));
    assertThat(legacyValue, equalTo(3L));
  }

  @Test
  public void shouldRecordNumberOfQueriesInREBALANCINGState() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(3, State.REBALANCING));

    final long value = getLongMetricValue("REBALANCING-queries");
    final long legacyValue = getLongMetricValueLegacy("REBALANCING-queries");

    assertThat(value, equalTo(3L));
    assertThat(legacyValue, equalTo(3L));
  }

  @Test
  public void shouldRecordNumberOfQueriesInPENDING_SHUTDOWNGState() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(3, State.PENDING_SHUTDOWN));

    final long value = getLongMetricValue("PENDING_SHUTDOWN-queries");
    final long legacyValue = getLongMetricValueLegacy("PENDING_SHUTDOWN-queries");

    assertThat(value, equalTo(3L));
    assertThat(legacyValue, equalTo(3L));
  }

  @Test
  public void shouldRecordNumberOfQueriesInERRORState() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(3, State.ERROR));

    final long value = getLongMetricValue("ERROR-queries");
    final long legacyValue = getLongMetricValueLegacy("ERROR-queries");

    assertThat(value, equalTo(3L));
    assertThat(legacyValue, equalTo(3L));
  }

  @Test
  public void shouldRecordNumberOfQueriesInNOT_RUNNINGtate() {
    when(ksqlEngine.getPersistentQueries())
        .then(returnQueriesInState(4, State.NOT_RUNNING));

    final long value = getLongMetricValue("NOT_RUNNING-queries");
    final long legacyValue = getLongMetricValueLegacy("NOT_RUNNING-queries");

    assertThat(value, equalTo(4L));
    assertThat(legacyValue, equalTo(4L));
  }

  @Test
  public void shouldRecordNumberOfPersistentQueries() {
    when(ksqlEngine.getPersistentQueries()).then(returnQueriesInState(3, State.RUNNING));

    final double value = getMetricValue("num-persistent-queries");
    final double legacyValue = getMetricValueLegacy("num-persistent-queries");

    assertThat(value, equalTo(3.0));
    assertThat(legacyValue, equalTo(3.0));
  }

  @Test
  public void shouldRecordMessagesConsumed() {
    final int numMessagesConsumed = 500;
    consumeMessages(numMessagesConsumed, "group1");
    engineMetrics.updateMetrics();

    final double value = getMetricValue("messages-consumed-per-sec");
    final double legacyValue = getMetricValueLegacy("messages-consumed-per-sec");

    assertThat(Math.floor(value), closeTo(numMessagesConsumed / 100, 0.01));
    assertThat(Math.floor(legacyValue), closeTo(numMessagesConsumed / 100, 0.01));
  }

  @Test
  public void shouldRecordMessagesProduced() {
    final int numMessagesProduced = 500;
    produceMessages(numMessagesProduced);
    engineMetrics.updateMetrics();

    final double value = getMetricValue("messages-produced-per-sec");
    final double legacyValue = getMetricValueLegacy("messages-produced-per-sec");

    assertThat(Math.floor(value), closeTo(numMessagesProduced / 100, 0.01));
    assertThat(Math.floor(legacyValue), closeTo(numMessagesProduced / 100, 0.01));
  }

  @Test
  public void shouldRecordMaxMessagesConsumedByQuery() {
    final int numMessagesConsumed = 500;
    consumeMessages(numMessagesConsumed, "group1");
    consumeMessages(numMessagesConsumed * 100, "group2");
    engineMetrics.updateMetrics();

    final double value = getMetricValue("messages-consumed-max");
    final double legacyValue = getMetricValueLegacy("messages-consumed-max");

    assertThat(Math.floor(value), closeTo(numMessagesConsumed, 5.0));
    assertThat(Math.floor(legacyValue), closeTo(numMessagesConsumed, 5.0));
  }

  @Test
  public void shouldRecordMinMessagesConsumedByQuery() {
    final int numMessagesConsumed = 500;
    consumeMessages(numMessagesConsumed, "group1");
    consumeMessages(numMessagesConsumed * 100, "group2");
    engineMetrics.updateMetrics();

    final double value = getMetricValue("messages-consumed-min");
    final double legacyValue = getMetricValueLegacy("messages-consumed-min");

    assertThat(Math.floor(value), closeTo(numMessagesConsumed / 100, 0.01));
    assertThat(Math.floor(legacyValue), closeTo(numMessagesConsumed / 100, 0.01));
  }

  @Test
  public void shouldRecordCustomMetric() {
    final double value = getMetricValue("my-custom-metric");
    final double legacyValue = getMetricValueLegacy("my-custom-metric");

    assertThat(value, equalTo(123.0));
    assertThat(legacyValue, equalTo(123.0));
  }

  private double getMetricValue(final String metricName) {
    final Metrics metrics = engineMetrics.getMetrics();
    return Double.parseDouble(
        metrics.metric(
            metrics.metricName(
                metricName, metricNamePrefix + METRIC_GROUP + "-query-stats", CUSTOM_TAGS_WITH_SERVICE_ID)
        ).metricValue().toString()
    );
  }

  private long getLongMetricValue(final String metricName) {
    final Metrics metrics = engineMetrics.getMetrics();
    return Long.parseLong(
        metrics.metric(
            metrics.metricName(
                metricName, metricNamePrefix + METRIC_GROUP + "-query-stats", CUSTOM_TAGS_WITH_SERVICE_ID)
        ).metricValue().toString()
    );
  }
  
  private double getMetricValueLegacy(final String metricName) {
    final Metrics metrics = engineMetrics.getMetrics();
    return Double.parseDouble(
        metrics.metric(
            metrics.metricName(
                metricName, legacyMetricNamePrefix + METRIC_GROUP + "-query-stats", CUSTOM_TAGS)
        ).metricValue().toString()
    );
  }

  private long getLongMetricValueLegacy(final String metricName) {
    final Metrics metrics = engineMetrics.getMetrics();
    return Long.parseLong(
        metrics.metric(
            metrics.metricName(
                metricName, legacyMetricNamePrefix + METRIC_GROUP + "-query-stats", CUSTOM_TAGS)
        ).metricValue().toString()
    );
  }

  private void consumeMessages(final int numMessages, final String groupId) {
    final ConsumerCollector collector1 = new ConsumerCollector();
    collector1.configure(
        ImmutableMap.of(
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            KsqlConfig.KSQL_INTERNAL_METRIC_COLLECTORS_CONFIG, metricCollectors
        )
    );
    final Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = new HashMap<>();
    final List<ConsumerRecord<Object, Object>> recordList = new ArrayList<>();
    for (int i = 0; i < numMessages; i++) {
      recordList.add(new ConsumerRecord<>("foo", 1, 1, 1L, TimestampType
                .CREATE_TIME, 10, 10,
          "key", "1234567890", new RecordHeaders(), Optional.empty()));
    }
    records.put(new TopicPartition("foo", 1), recordList);
    final ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);
    collector1.onConsume(consumerRecords);
  }

  private void produceMessages(final int numMessages) {
    final ProducerCollector collector1 = new ProducerCollector();
    collector1.configure(
        ImmutableMap.of(
            ProducerConfig.CLIENT_ID_CONFIG, "client1",
            KsqlConfig.KSQL_INTERNAL_METRIC_COLLECTORS_CONFIG, metricCollectors
        )
    );
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
        when(query.getState()).thenReturn(state);
        queryMetadataList.add(query);
      }
      return queryMetadataList;
    };
  }

  private static class TestKsqlMetricsExtension implements KsqlMetricsExtension {

    @Override
    public void configure(final Map<String, ?> config) {
    }

    @Override
    public List<KsqlMetric> getCustomMetrics() {
      final String name = "my-custom-metric";
      final String description = "";
      final Supplier<MeasurableStat> statSupplier =
          () -> new MeasurableStat() {
            @Override
            public double measure(final MetricConfig metricConfig, final long l) {
              return 123;
            }

            @Override
            public void record(final MetricConfig metricConfig, final double v, final long l) {
              // Nothing to record
            }
          };
      return ImmutableList.of(KsqlMetric.of(name, description, statSupplier));
    }
  }
}
