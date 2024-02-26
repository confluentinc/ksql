/*
 * Copyright 2021 Confluent Inc.
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

import static io.confluent.ksql.util.KsqlConstants.KSQL_QUERY_ROUTING_TYPE_TAG;
import static io.confluent.ksql.util.KsqlConstants.KSQL_QUERY_SOURCE_TAG;
import static io.confluent.ksql.util.KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.util.KsqlConstants.QuerySourceType;
import io.confluent.ksql.util.KsqlConstants.RoutingNodeType;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.Map;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScalablePushQueryMetricsTest {

  private ScalablePushQueryMetrics scalablePushQueryMetrics;
  private static final String KSQL_SERVICE_ID = "test-ksql-service-id";
  private static final Map<String, String> CUSTOM_TAGS = ImmutableMap
      .of("tag1", "value1", "tag2", "value2");
  private static final Map<String, String> CUSTOM_TAGS_WITH_SERVICE_ID = ImmutableMap
      .of("tag1", "value1", "tag2", "value2", KSQL_SERVICE_ID_METRICS_TAG, KSQL_SERVICE_ID);

  @Mock
  private KsqlEngine ksqlEngine;

  @Mock
  private Time time;

  @Before
  public void setUp() {
    when(ksqlEngine.getServiceId()).thenReturn(KSQL_SERVICE_ID);
    when(time.nanoseconds()).thenReturn(6000L);

    scalablePushQueryMetrics = new ScalablePushQueryMetrics(ksqlEngine.getServiceId(), CUSTOM_TAGS, time, new Metrics());
  }

  @After
  public void tearDown() {
    scalablePushQueryMetrics.close();
  }

  @Test
  public void shouldRemoveAllSensorsOnClose() {
    assertTrue(scalablePushQueryMetrics.getSensors().size() > 0);

    scalablePushQueryMetrics.close();

    scalablePushQueryMetrics.getSensors().forEach(
        sensor -> assertThat(scalablePushQueryMetrics.getMetrics().getSensor(sensor.name()), is(nullValue())));
  }

  @Test
  public void shouldRecordNumberOfLocalRequests() {
    // Given:
    scalablePushQueryMetrics.recordLocalRequests(3);

    // When:
    final double value = getMetricValue("-local-count");
    final double rate = getMetricValue("-local-rate");

    // Then:
    assertThat(value, equalTo(1.0));
    assertThat(rate, greaterThan(0.0));
  }

  @Test
  public void shouldRecordNumberOfRemoteRequests() {
    // Given:
    scalablePushQueryMetrics.recordRemoteRequests(3);

    // When:
    final double value = getMetricValue("-remote-count");
    final double rate = getMetricValue("-remote-rate");

    // Then:
    assertThat(value, equalTo(1.0));
    assertThat(rate, greaterThan(0.0));
  }

  @Test
  public void shouldRecordErrorRate() {
    // Given:
    scalablePushQueryMetrics.recordErrorRate(3, QuerySourceType.NON_WINDOWED,
        RoutingNodeType.SOURCE_NODE);

    // When:
    final double value = getMetricValue("-error-total");
    final double rate = getMetricValue("-error-rate");
    final double detailedValue = getMetricValue("-detailed-error-total",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);

    // Then:
    assertThat(value, equalTo(1.0));
    assertThat(rate, greaterThan(0.0));
    assertThat(detailedValue, equalTo(1.0));
  }

  @Test
  public void shouldRecordResponseSize() {
    // Given:
    scalablePushQueryMetrics.recordResponseSize(1500, QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);

    // When:
    final double value = getMetricValue("-response-size");
    final double detailedValue = getMetricValue("-detailed-response-size",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);

    // Then:
    assertThat(value, equalTo(1500.0));
    assertThat(detailedValue, equalTo(1500.0));
  }

  @Test
  public void shouldRecordConnectionDuration() {
    // Given:
    scalablePushQueryMetrics.recordConnectionDuration(3000, QuerySourceType.NON_WINDOWED,
        RoutingNodeType.SOURCE_NODE);

    // When:
    final double avg = getMetricValue("-connection-duration-avg");
    final double max = getMetricValue("-connection-duration-max");
    final double min = getMetricValue("-connection-duration-min");
    final double total = getMetricValue("-total");
    final double detailedAvg = getMetricValue("-detailed-connection-duration-avg",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);
    final double detailedMax = getMetricValue("-detailed-connection-duration-max",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);
    final double detailedMin = getMetricValue("-detailed-connection-duration-min",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);
    final double detailedTotal = getMetricValue("-detailed-total",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);

    // Then:
    assertThat(avg, is(3.0));
    assertThat(min, is(3.0));
    assertThat(max, is(3.0));
    assertThat(total, is(1.0));
    assertThat(detailedAvg, is(3.0));
    assertThat(detailedMin, is(3.0));
    assertThat(detailedMax, is(3.0));
    assertThat(detailedTotal, is(1.0));
  }

  @Test
  public void shouldRecordConnectionDurationPercentiles() {
    // Given:
    when(time.nanoseconds()).thenReturn(600000000L);
    scalablePushQueryMetrics.recordConnectionDuration(100000000L, QuerySourceType.NON_WINDOWED,
        RoutingNodeType.SOURCE_NODE);
    scalablePushQueryMetrics.recordConnectionDuration(200000000L, QuerySourceType.NON_WINDOWED,
        RoutingNodeType.SOURCE_NODE);
    scalablePushQueryMetrics.recordConnectionDuration(300000000L, QuerySourceType.NON_WINDOWED,
        RoutingNodeType.SOURCE_NODE);
    scalablePushQueryMetrics.recordConnectionDuration(400000000L, QuerySourceType.NON_WINDOWED,
        RoutingNodeType.SOURCE_NODE);
    scalablePushQueryMetrics.recordConnectionDuration(500000000L, QuerySourceType.NON_WINDOWED,
        RoutingNodeType.SOURCE_NODE);

    // When:
    final double detailed50 = getMetricValue("-detailed-distribution-50",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);
    final double detailed75 = getMetricValue("-detailed-distribution-75",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);
    final double detailed90 = getMetricValue("-detailed-distribution-90",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);
    final double detailed99 = getMetricValue("-detailed-distribution-99",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);

    // Then:
    assertThat(detailed50, closeTo(297857.85, 0.1));
    assertThat(detailed75, closeTo(398398.39, 0.1));
    assertThat(detailed90, closeTo(495555.55, 0.1));
    assertThat(detailed99, closeTo(495555.55, 0.1));
  }

  @Test
  public void shouldRecordStatus() {
    // Given:
    scalablePushQueryMetrics.recordStatusCode(200);
    scalablePushQueryMetrics.recordStatusCode(200);
    scalablePushQueryMetrics.recordStatusCode(401);
    scalablePushQueryMetrics.recordStatusCode(401);
    scalablePushQueryMetrics.recordStatusCode(401);
    scalablePushQueryMetrics.recordStatusCode(502);

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
    scalablePushQueryMetrics.recordRowsReturned(12, QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);

    // When:
    final double detailedValue = getMetricValue("-rows-returned-total",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);

    // Then:
    assertThat(detailedValue, equalTo(12.0));
  }

  @Test
  public void shouldRecordRowsProcessed() {
    // Given:
    scalablePushQueryMetrics.recordRowsProcessed(1399, QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);

    // When:
    final double detailedValue = getMetricValue("-rows-processed-total",
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);

    // Then:
    assertThat(detailedValue, equalTo(1399.0));
  }

  private double getMetricValue(final String metricName) {
    final Metrics metrics = scalablePushQueryMetrics.getMetrics();
    return Double.parseDouble(
        metrics.metric(
            metrics.metricName(
                "scalable-push-query-requests" + metricName,
                ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "scalable-push-query",
                CUSTOM_TAGS_WITH_SERVICE_ID)
        ).metricValue().toString()
    );
  }

  private double getMetricValue(
      final String metricName,
      final QuerySourceType sourceType,
      final RoutingNodeType routingNodeType
  ) {
    final Metrics metrics = scalablePushQueryMetrics.getMetrics();
    final Map<String, String> tags = ImmutableMap.<String, String>builder()
        .putAll(CUSTOM_TAGS_WITH_SERVICE_ID)
        .put(KSQL_QUERY_SOURCE_TAG, sourceType.name().toLowerCase())
        .put(KSQL_QUERY_ROUTING_TYPE_TAG, routingNodeType.name().toLowerCase())
        .build();
    return Double.parseDouble(
        metrics.metric(
            metrics.metricName(
                "scalable-push-query-requests" + metricName,
                ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "scalable-push-query",
                tags)
        ).metricValue().toString()
    );
  }

}
