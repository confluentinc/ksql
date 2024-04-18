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

import static com.google.common.testing.NullPointerTester.Visibility.PACKAGE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.QueryMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.streams.KafkaStreams.State;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class QueryStateMetricsReportingListenerTest {
  private static final MetricName METRIC_NAME_1 =
      new MetricName("bob", "g1", "d1", ImmutableMap.of());
  private static final MetricName METRIC_NAME_2 =
      new MetricName("dylan", "g1", "d1", ImmutableMap.of());
  private static final MetricName METRIC_NAME_3 =
      new MetricName("steven", "g1", "d1", ImmutableMap.of());
  private static final MetricName METRIC_NAME_4 =
          new MetricName("pete", "g1", "d1", ImmutableMap.of());
  private static final QueryId QUERY_ID = new QueryId("foo");
  private static final String TAG = "_confluent-ksql-" + "some-prefix-" + "query_" + QUERY_ID.toString();

  @Mock
  private Metrics metrics;
  @Mock
  private QueryMetadata query;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private MetaStore metaStore;
  @Captor
  private ArgumentCaptor<Gauge<String>> gaugeCaptor;
  private QueryStateMetricsReportingListener listener;

  private final Map<String, String> metricsTags = Collections.singletonMap("tag1", "value1");

  @Before
  public void setUp() {
    when(metrics.metricName(any(), any(), any(), anyMap()))
        .thenReturn(METRIC_NAME_1)
        .thenReturn(METRIC_NAME_2)
        .thenReturn(METRIC_NAME_3)
        .thenReturn(METRIC_NAME_4);
    when(query.getQueryId()).thenReturn(QUERY_ID);

    listener = new QueryStateMetricsReportingListener(metrics, "some-prefix-", metricsTags);
  }

  @Test
  public void shouldThrowOnNullParams() {
    new NullPointerTester().testConstructors(QueryStateMetricsReportingListener.class, PACKAGE);
  }

  @Test
  public void shouldAddMetricOnCreation() {
    // When:
    listener.onCreate(serviceContext, metaStore, query);
    final Map<String, String> tags = new HashMap<>(metricsTags);
    tags.put("status", TAG);

    // Then:
    verify(metrics).metricName("query-status", "some-prefix-ksql-queries",
        "The current Kafka Streams status of the given query.",
        tags);
    verify(metrics).metricName("error-status", "some-prefix-ksql-queries",
        "The current error status of the given query, if the state is in ERROR state",
        tags);
    verify(metrics).metricName("ksql-query-status", "some-prefix-ksql-queries",
            "The current ksqlDB status of the given query.",
            tags);
    tags.put("query-id", QUERY_ID.toString());
    verify(metrics).metricName(QueryStateMetricsReportingListener.QUERY_RESTART_METRIC_NAME, "some-prefix-ksql-queries",
        QueryStateMetricsReportingListener.QUERY_RESTART_METRIC_DESCRIPTION,
        tags);

    verify(metrics).addMetric(eq(METRIC_NAME_1), isA(Gauge.class));
    verify(metrics).addMetric(eq(METRIC_NAME_2), isA(Gauge.class));
    verify(metrics).addMetric(eq(METRIC_NAME_3), isA(CumulativeSum.class));
    verify(metrics).addMetric(eq(METRIC_NAME_4), isA(Gauge.class));
  }

  @Test
  public void shouldGracefullyHandleStateCallbackAfterDeregister() {
    // Given:
    listener.onCreate(serviceContext, metaStore, query);
    listener.onDeregister(query);

    // When/Then(don't throw)
    listener.onStateChange(query, State.RUNNING, State.NOT_RUNNING);
  }

  @Test
  public void shouldGracefullyHandleErrorCallbackAfterDeregister() {
    // Given:
    listener.onCreate(serviceContext, metaStore, query);
    listener.onDeregister(query);

    // When/Then(don't throw)
    listener.onError(query, new QueryError(123, "foo", Type.USER));
  }

  @Test
  public void shouldAddMetricWithSuppliedPrefix() {
    // Given:
    final String groupPrefix = "some-prefix-";
    final Map<String, String> tags = new HashMap<>(metricsTags);
    tags.put("status", TAG);

    clearInvocations(metrics);

    // When:
    listener = new QueryStateMetricsReportingListener(metrics, groupPrefix, metricsTags);
    listener.onCreate(serviceContext, metaStore, query);

    // Then:
    verify(metrics).metricName("query-status", groupPrefix + "ksql-queries",
        "The current Kafka Streams status of the given query.",
        tags);
    verify(metrics).metricName("error-status", groupPrefix + "ksql-queries",
        "The current error status of the given query, if the state is in ERROR state",
        tags);
    verify(metrics).metricName("ksql-query-status", groupPrefix + "ksql-queries",
            "The current ksqlDB status of the given query.",
            tags);
    tags.put("query-id", QUERY_ID.toString());
    verify(metrics).metricName(QueryStateMetricsReportingListener.QUERY_RESTART_METRIC_NAME, "some-prefix-ksql-queries",
        QueryStateMetricsReportingListener.QUERY_RESTART_METRIC_DESCRIPTION,
        tags);
  }

  @Test
  public void shouldInitiallyHaveInitialState() {
    // When:
    listener.onCreate(serviceContext, metaStore, query);

    // Then:
    assertThat(currentGaugeValue(METRIC_NAME_1), is("-"));
    assertThat(currentGaugeValue(METRIC_NAME_2), is("NO_ERROR"));
  }

  @Test
  public void shouldUpdateToNewState() {
    // When:
    when(query.getQueryStatus()).thenReturn(KsqlConstants.KsqlQueryStatus.RUNNING);
    listener.onCreate(serviceContext, metaStore, query);
    listener.onStateChange(query, State.REBALANCING, State.RUNNING);

    // Then:
    assertThat(currentGaugeValue(METRIC_NAME_1), is("REBALANCING"));
  }

  @Test
  public void shouldUpdateOnError() {
    // When:
    when(query.getQueryStatus()).thenReturn(KsqlConstants.KsqlQueryStatus.RUNNING);
    listener.onCreate(serviceContext, metaStore, query);
    listener.onError(query, new QueryError(1, "foo", Type.USER));

    // Then:
    assertThat(currentGaugeValue(METRIC_NAME_2), is("USER"));
  }

  @Test
  public void shouldReportKsqlQueryStatus() {
    // When:
    listener.onCreate(serviceContext, metaStore, query);

    when(query.getQueryStatus())
            .thenReturn(KsqlConstants.KsqlQueryStatus.RUNNING)
            .thenReturn(KsqlConstants.KsqlQueryStatus.PAUSED)
            .thenReturn(KsqlConstants.KsqlQueryStatus.UNRESPONSIVE)
            .thenReturn(KsqlConstants.KsqlQueryStatus.ERROR);

    // Then:
    listener.onKsqlStateChange(query);
    assertThat(currentGaugeValue(METRIC_NAME_4), is("RUNNING"));
    listener.onKsqlStateChange(query);
    assertThat(currentGaugeValue(METRIC_NAME_4), is("PAUSED"));
    listener.onKsqlStateChange(query);
    assertThat(currentGaugeValue(METRIC_NAME_4), is("UNRESPONSIVE"));
    listener.onKsqlStateChange(query);
    assertThat(currentGaugeValue(METRIC_NAME_4), is("ERROR"));
  }

  @Test
  public void shouldRemoveMetricOnClose() {
    // When:
    listener.onCreate(serviceContext, metaStore, query);
    listener.onDeregister(query);

    // Then:
    verify(metrics).removeMetric(METRIC_NAME_1);
    verify(metrics).removeMetric(METRIC_NAME_2);
    verify(metrics).removeMetric(METRIC_NAME_3);
    verify(metrics).removeMetric(METRIC_NAME_4);
  }

  private String currentGaugeValue(final MetricName name) {
    verify(metrics).addMetric(eq(name), gaugeCaptor.capture());
    return gaugeCaptor.getValue().value(null, 0L);
  }
}