/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullSourceType;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.RoutingNodeType;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullQueryMetricsTest {

  private PullQueryExecutorMetrics pullMetrics;
  private static final String KSQL_SERVICE_ID = "test-ksql-service-id";
  private static final Map<String, String> CUSTOM_TAGS = ImmutableMap
      .of("tag1", "value1", "tag2", "value2");
  private static final Map<String, String> CUSTOM_TAGS_WITH_SERVICE_ID = ImmutableMap
      .of("tag1", "value1", "tag2", "value2", KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, KSQL_SERVICE_ID);

  @Mock
  private KsqlEngine ksqlEngine;

  @Mock
  private Time time;

  @Before
  public void setUp() {
    MetricCollectors.initialize();
    when(ksqlEngine.getServiceId()).thenReturn(KSQL_SERVICE_ID);
    when(time.nanoseconds()).thenReturn(6000L);

    pullMetrics = new PullQueryExecutorMetrics(ksqlEngine.getServiceId(), CUSTOM_TAGS, time);
  }

  @After
  public void tearDown() {
    pullMetrics.close();
    MetricCollectors.cleanUp();
  }

  @Test
  public void shouldRemoveAllSensorsOnClose() {
    assertTrue(pullMetrics.getSensors().size() > 0);

    pullMetrics.close();

    pullMetrics.getSensors().forEach(
        sensor -> assertThat(pullMetrics.getMetrics().getSensor(sensor.name()), is(nullValue())));
  }

  @Test
  public void shouldRecordNumberOfLocalRequests() {
    // Given:
    pullMetrics.recordLocalRequests(3);

    // When:
    final double value = getMetricValue("-local-count");
    final double rate = getMetricValue("-local-rate");
    final double legacyValue = getMetricValueLegacy("-local-count");
    final double legacyRate = getMetricValueLegacy("-local-rate");

    // Then:
    assertThat(value, equalTo(1.0));
    assertThat(rate, closeTo(0.03, 0.001));
    assertThat(legacyValue, equalTo(1.0));
    assertThat(legacyRate, closeTo(0.03, 0.001));
  }

  @Test
  public void shouldRecordNumberOfRemoteRequests() {
    // Given:
    pullMetrics.recordRemoteRequests(3);

    // When:
    final double value = getMetricValue("-remote-count");
    final double rate = getMetricValue("-remote-rate");
    final double legacyValue = getMetricValueLegacy("-remote-count");
    final double legacyRate = getMetricValueLegacy("-remote-rate");

    // Then:
    assertThat(value, equalTo(1.0));
    assertThat(rate, closeTo(0.03, 0.001));
    assertThat(legacyValue, equalTo(1.0));
    assertThat(legacyRate, closeTo(0.03, 0.001));
  }

  @Test
  public void shouldRecordErrorRate() {
    // Given:
    pullMetrics.recordErrorRate(3, PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP,
        RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);

    // When:
    final double value = getMetricValue("-error-total");
    final double rate = getMetricValue("-error-rate");
    final double legacyValue = getMetricValueLegacy("-error-total");
    final double legacyRate = getMetricValueLegacy("-error-rate");
    final double detailedValue = getMetricValue("-detailed-error-total",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);

    // Then:
    assertThat(value, equalTo(1.0));
    assertThat(rate, closeTo(0.03, 0.001));
    assertThat(legacyValue, equalTo(1.0));
    assertThat(legacyRate, closeTo(0.03, 0.001));
    assertThat(detailedValue, equalTo(1.0));
  }

  @Test
  public void shouldRecordResponseSize() {
    // Given:
    pullMetrics.recordResponseSize(1500, PullSourceType.NON_WINDOWED,
        PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);

    // When:
    final double value = getMetricValue("-response-size");
    final double detailedValue = getMetricValue("-detailed-response-size",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);

    // Then:
    assertThat(value, equalTo(1500.0));
    assertThat(detailedValue, equalTo(1500.0));
  }

  @Test
  public void shouldRecordRequestRate() {
    // Given:
    pullMetrics.recordLatency(3000, PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP,
        RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);
    pullMetrics.recordLatency(3000, PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP,
        RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);
    pullMetrics.recordLatency(3000, PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP,
        RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);

    // When:
    final double rate = getMetricValue("-rate");
    final double legacyRate = getMetricValueLegacy("-rate");

    // Then:
    assertThat(rate, closeTo(0.03, 0.001));
    assertThat(legacyRate, closeTo(0.03, 0.001));
  }

  @Test
  public void shouldRecordLatency() {
    // Given:
    pullMetrics.recordLatency(3000, PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP,
        RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);

    // When:
    final double avg = getMetricValue("-latency-avg");
    final double max = getMetricValue("-latency-max");
    final double min = getMetricValue("-latency-min");
    final double total = getMetricValue("-total");
    final double legacyAvg = getMetricValueLegacy("-latency-avg");
    final double legacyMax = getMetricValueLegacy("-latency-max");
    final double legacyMin = getMetricValueLegacy("-latency-min");
    final double legacyTotal = getMetricValueLegacy("-total");
    final double detailedAvg = getMetricValue("-detailed-latency-avg",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);
    final double detailedMax = getMetricValue("-detailed-latency-max",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);
    final double detailedMin = getMetricValue("-detailed-latency-min",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);
    final double detailedTotal = getMetricValue("-detailed-total",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);

    // Then:
    assertThat(avg, is(3.0));
    assertThat(min, is(3.0));
    assertThat(max, is(3.0));
    assertThat(total, is(1.0));
    assertThat(legacyAvg, is(3.0));
    assertThat(legacyMin, is(3.0));
    assertThat(legacyMax, is(3.0));
    assertThat(legacyTotal, is(1.0));
    assertThat(detailedAvg, is(3.0));
    assertThat(detailedMin, is(3.0));
    assertThat(detailedMax, is(3.0));
    assertThat(detailedTotal, is(1.0));
  }

  @Test
  public void shouldRecordLatencyPercentiles() {
    // Given:
    when(time.nanoseconds()).thenReturn(600000000L);
    pullMetrics.recordLatency(100000000L, PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP,
        RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);
    pullMetrics.recordLatency(200000000L, PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP,
        RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);
    pullMetrics.recordLatency(300000000L, PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP,
        RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);
    pullMetrics.recordLatency(400000000L, PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP,
        RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);
    pullMetrics.recordLatency(500000000L, PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP,
        RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);

    // When:
    final double detailed50 = getMetricValue("-detailed-distribution-50",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);
    final double detailed75 = getMetricValue("-detailed-distribution-75",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);
    final double detailed90 = getMetricValue("-detailed-distribution-90",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);
    final double detailed99 = getMetricValue("-detailed-distribution-99",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);

    // Then:
    assertThat(detailed50, closeTo(297857.85, 0.1));
    assertThat(detailed75, closeTo(398398.39, 0.1));
    assertThat(detailed90, closeTo(495555.55, 0.1));
    assertThat(detailed99, closeTo(495555.55, 0.1));
  }

  @Test
  public void shouldRecordStatus() {
    // Given:
    pullMetrics.recordStatusCode(200);
    pullMetrics.recordStatusCode(200);
    pullMetrics.recordStatusCode(401);
    pullMetrics.recordStatusCode(401);
    pullMetrics.recordStatusCode(401);
    pullMetrics.recordStatusCode(502);

    // When:
    final double total2XX = getMetricValue("-2XX-total");
    final double total4XX = getMetricValue("-4XX-total");
    final double total5XX = getMetricValue("-5XX-total");

    // Then:
    assertThat(total2XX, is(2.0));
    assertThat(total4XX, is(3.0));
    assertThat(total5XX, is(1.0));
  }

  @Test
  public void shouldRecordRowsReturned() {
    // Given:
    pullMetrics.recordRowsReturned(12, PullSourceType.NON_WINDOWED,
        PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);

    // When:
    final double detailedValue = getMetricValue("-rows-returned-total",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);

    // Then:
    assertThat(detailedValue, equalTo(12.0));
  }

  @Test
  public void shouldRecordRowsProcessed() {
    // Given:
    pullMetrics.recordRowsProcessed(1399, PullSourceType.NON_WINDOWED,
        PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE, DataSourceType.KTABLE);

    // When:
    final double detailedValue = getMetricValue("-rows-processed-total",
        PullSourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        DataSourceType.KTABLE);

    // Then:
    assertThat(detailedValue, equalTo(1399.0));
  }

  private double getMetricValue(final String metricName) {
    final Metrics metrics = pullMetrics.getMetrics();
    return Double.parseDouble(
        metrics.metric(
            metrics.metricName(
                "pull-query-requests" + metricName,
                ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "pull-query",
                CUSTOM_TAGS_WITH_SERVICE_ID)
        ).metricValue().toString()
    );
  }

  private double getMetricValue(
      final String metricName,
      final PullSourceType sourceType,
      final PullPhysicalPlanType planType,
      final RoutingNodeType routingNodeType,
      final DataSourceType dataSourceType
  ) {
    final Metrics metrics = pullMetrics.getMetrics();
    final Map<String, String> tags = ImmutableMap.<String, String>builder()
        .putAll(CUSTOM_TAGS_WITH_SERVICE_ID)
        .put(KsqlConstants.KSQL_QUERY_SOURCE_TAG, sourceType.name().toLowerCase())
        .put(KsqlConstants.KSQL_QUERY_PLAN_TYPE_TAG, planType.name().toLowerCase())
        .put(KsqlConstants.KSQL_QUERY_ROUTING_TYPE_TAG, routingNodeType.name().toLowerCase())
        .put(KsqlConstants.KSQL_DATA_SOURCE_TYPE_TAG, dataSourceType.name().toLowerCase())
        .build();
    return Double.parseDouble(
        metrics.metric(
            metrics.metricName(
                "pull-query-requests" + metricName,
                ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "pull-query",
                tags)
        ).metricValue().toString()
    );
  }

  private double getMetricValueLegacy(final String metricName) {
    final Metrics metrics = pullMetrics.getMetrics();
    return Double.parseDouble(
        metrics.metric(
            metrics.metricName(
                "pull-query-requests" + metricName,
                ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + ksqlEngine.getServiceId()+ "pull-query",
                CUSTOM_TAGS)
        ).metricValue().toString()
    );
  }

}
