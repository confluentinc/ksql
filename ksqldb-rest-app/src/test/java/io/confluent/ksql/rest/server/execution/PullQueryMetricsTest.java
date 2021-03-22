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
import io.confluent.ksql.metrics.MetricCollectors;
import java.util.Map;

import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.ReservedInternalTopics;
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
    pullMetrics.recordErrorRate(3);

    // When:
    final double value = getMetricValue("-error-total");
    final double rate = getMetricValue("-error-rate");
    final double legacyValue = getMetricValueLegacy("-error-total");
    final double legacyRate = getMetricValueLegacy("-error-rate");

    // Then:
    assertThat(value, equalTo(1.0));
    assertThat(rate, closeTo(0.03, 0.001));
    assertThat(legacyValue, equalTo(1.0));
    assertThat(legacyRate, closeTo(0.03, 0.001));
  }

  @Test
  public void shouldRecordRequestRate() {
    // Given:
    pullMetrics.recordLatency(3000);
    pullMetrics.recordLatency(3000);
    pullMetrics.recordLatency(3000);

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
    pullMetrics.recordLatency(3000);

    // When:
    final double avg = getMetricValue("-latency-avg");
    final double max = getMetricValue("-latency-max");
    final double min = getMetricValue("-latency-min");
    final double total = getMetricValue("-total");
    final double legacyAvg = getMetricValueLegacy("-latency-avg");
    final double legacyMax = getMetricValueLegacy("-latency-max");
    final double legacyMin = getMetricValueLegacy("-latency-min");
    final double legacyTotal = getMetricValueLegacy("-total");

    // Then:
    assertThat(avg, is(3.0));
    assertThat(min, is(3.0));
    assertThat(max, is(3.0));
    assertThat(total, is(1.0));
    assertThat(legacyAvg, is(3.0));
    assertThat(legacyMin, is(3.0));
    assertThat(legacyMax, is(3.0));
    assertThat(legacyTotal, is(1.0));
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
