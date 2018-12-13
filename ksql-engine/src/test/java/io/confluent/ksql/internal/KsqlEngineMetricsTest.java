/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.confluent.ksql.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.metrics.ProducerCollector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
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
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KsqlEngineMetricsTest {

  private static final String METRIC_GROUP = "testGroup";
  private KsqlEngine ksqlEngine;
  private KsqlEngineMetrics engineMetrics;
  private final String ksqlServiceId = "test-ksql-service-id";
  private final String metricNamePrefix = KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX + ksqlServiceId;

  @Before
  public void setUp() {
    MetricCollectors.initialize();
    ksqlEngine = EasyMock.niceMock(KsqlEngine.class);
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(KsqlConfig.KSQL_SERVICE_ID_CONFIG, ksqlServiceId));
    EasyMock.expect(ksqlEngine.getServiceId()).andReturn(ksqlServiceId);
    EasyMock.replay(ksqlEngine);
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
      assertTrue(engineMetrics.getMetrics().getSensor(sensor.name()) == null);
    });
  }

  @Test
  public void shouldRecordNumberOfActiveQueries() {
    EasyMock.reset(ksqlEngine);
    EasyMock.expect(ksqlEngine.numberOfLiveQueries()).andReturn(3L);
    EasyMock.replay(ksqlEngine);
    final double value = getMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "num-active-queries");
    assertEquals(3.0, value, 0.0);
  }

  @Test
  public void shouldRecordNumberOfQueriesInCREATEDState() {
    EasyMock.reset(ksqlEngine);
    EasyMock.expect(ksqlEngine.getPersistentQueries()).andReturn(getMockQueryMetadataList(3, State.CREATED));
    EasyMock.replay(ksqlEngine);
    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-CREATED-queries");
    assertEquals(3L, value);
  }

  @Test
  public void shouldRecordNumberOfQueriesInRUNNINGState() {
    EasyMock.reset(ksqlEngine);
    EasyMock.expect(ksqlEngine.getPersistentQueries()).andReturn(getMockQueryMetadataList(3, State.RUNNING));
    EasyMock.replay(ksqlEngine);
    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-RUNNING-queries");
    assertEquals(3L, value);
  }

  @Test
  public void shouldRecordNumberOfQueriesInREBALANCINGState() {
    EasyMock.reset(ksqlEngine);
    EasyMock.expect(ksqlEngine.getPersistentQueries()).andReturn(getMockQueryMetadataList(3, State.REBALANCING));
    EasyMock.replay(ksqlEngine);
    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-REBALANCING-queries");
    assertEquals(3L, value);
  }

  @Test
  public void shouldRecordNumberOfQueriesInPENDING_SHUTDOWNGState() {
    EasyMock.reset(ksqlEngine);
    EasyMock.expect(ksqlEngine.getPersistentQueries()).andReturn(getMockQueryMetadataList(3, State.PENDING_SHUTDOWN));
    EasyMock.replay(ksqlEngine);
    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-PENDING_SHUTDOWN-queries");
    assertEquals(3L, value);
  }

  @Test
  public void shouldRecordNumberOfQueriesInERRORState() {
    EasyMock.reset(ksqlEngine);
    EasyMock.expect(ksqlEngine.getPersistentQueries()).andReturn(getMockQueryMetadataList(3, State.ERROR));
    EasyMock.replay(ksqlEngine);
    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-ERROR-queries");
    assertEquals(3L, value);
  }

  @Test
  public void shouldRecordNumberOfQueriesInNOT_RUNNINGtate() {
    EasyMock.reset(ksqlEngine);
    EasyMock.expect(ksqlEngine.getPersistentQueries()).andReturn(getMockQueryMetadataList(3, State.NOT_RUNNING));
    EasyMock.replay(ksqlEngine);
    final long value = getLongMetricValue(engineMetrics.getMetrics(), metricNamePrefix + "testGroup-query-stats-NOT_RUNNING-queries");
    assertEquals(3L, value);
  }

  @Test
  public void shouldRecordNumberOfPersistentQueries() {
    EasyMock.reset(ksqlEngine);
    EasyMock.expect(ksqlEngine.numberOfPersistentQueries()).andReturn(3L);
    EasyMock.replay(ksqlEngine);
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

  private double getMetricValue(final Metrics metrics, final String metricName) {
    return Double.valueOf(
        metrics.metric(metrics.metricName(metricName, METRIC_GROUP + "-query-stats"))
            .metricValue().toString());
  }

  private long getLongMetricValue(final Metrics metrics, final String metricName) {
    return Long.valueOf(
        metrics.metric(metrics.metricName(metricName, METRIC_GROUP + "-query-stats"))
            .metricValue().toString());
  }

  private void consumeMessages(final int numMessages, final String groupId) {
    final ConsumerCollector collector1 = new ConsumerCollector();
    collector1.configure(ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, groupId));
    final Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records = new HashMap<>();
    final List<ConsumerRecord<Object, Object>> recordList = new ArrayList<>();
    for (int i = 0; i < numMessages; i++) {
      recordList.add(new ConsumerRecord<>("foo", 1, 1, 1l, TimestampType
          .CREATE_TIME, 1l, 10, 10, "key", "1234567890"));
    }
    records.put(new TopicPartition("foo", 1), recordList);
    final ConsumerRecords<Object, Object> consumerRecords = new ConsumerRecords<>(records);
    collector1.onConsume(consumerRecords);
  }

  private void produceMessages(final int numMessages) {
    final ProducerCollector collector1 = new ProducerCollector();
    collector1.configure(ImmutableMap.of(ProducerConfig.CLIENT_ID_CONFIG, "client1"));
    for (int i = 0; i < numMessages; i++) {
      collector1.onSend(new ProducerRecord<>("foo", "key", Integer.toString(i)));
    }
  }

  private List<PersistentQueryMetadata> getMockQueryMetadataList(
      final int numberOfQueries,
      final KafkaStreams.State state) {
    final List<PersistentQueryMetadata> queryMetadataList = new ArrayList<>();
    for (int i = 0; i < numberOfQueries; i++) {
      final PersistentQueryMetadata persistentQueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
      final KafkaStreams kafkaStreams = EasyMock.niceMock(KafkaStreams.class);
      EasyMock.expect(kafkaStreams.state()).andReturn(state);
      EasyMock.expect(persistentQueryMetadata.getKafkaStreams()).andReturn(kafkaStreams);
      EasyMock.replay(kafkaStreams, persistentQueryMetadata);
      queryMetadataList.add(persistentQueryMetadata);
    }
    return queryMetadataList;
  }
}
