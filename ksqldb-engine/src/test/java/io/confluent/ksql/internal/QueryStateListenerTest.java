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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Ticker;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryError.Type;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams.State;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RunWith(MockitoJUnitRunner.class)
public class QueryStateListenerTest {
  private static final long SOME_TIME = 1;
  private static final MetricName METRIC_NAME_1 =
      new MetricName("bob", "g1", "d1", ImmutableMap.of());
  private static final MetricName METRIC_NAME_2 =
      new MetricName("dylan", "g1", "d1", ImmutableMap.of());

  @Mock
  private Metrics metrics;
  @Mock
  private Ticker ticker;
  @Captor
  private ArgumentCaptor<Gauge<String>> gaugeCaptor;
  private QueryStateListener listener;

  @Before
  public void setUp() {
    when(metrics.metricName(any(), any(), any(), anyMap()))
        .thenReturn(METRIC_NAME_1)
        .thenReturn(METRIC_NAME_2);

    listener = new QueryStateListener(metrics, "", "app-id", ticker);
  }

  @Test
  public void shouldThrowOnNullParams() {
    new NullPointerTester().testConstructors(QueryStateListener.class, PACKAGE);
  }

  @Test
  public void shouldAddMetricOnCreation() {
    // When:
    // Listener created in setup

    // Then:
    verify(metrics).metricName("query-status", "ksql-queries",
        "The current status of the given query.",
        ImmutableMap.of("status", "app-id"));
    verify(metrics).metricName("error-status", "ksql-queries",
        "The current error status of the given query, if the state is in ERROR state",
        ImmutableMap.of("status", "app-id"));

    verify(metrics).addMetric(eq(METRIC_NAME_1), isA(Gauge.class));
    verify(metrics).addMetric(eq(METRIC_NAME_2), isA(Gauge.class));
  }

  @Test
  public void shouldAddMetricWithSuppliedPrefix() {
    // Given:
    final String groupPrefix = "some-prefix-";

    clearInvocations(metrics);

    // When:
    listener = new QueryStateListener(metrics, groupPrefix, "app-id");

    // Then:
    verify(metrics).metricName("query-status", groupPrefix + "ksql-queries",
        "The current status of the given query.",
        ImmutableMap.of("status", "app-id"));
    verify(metrics).metricName("error-status", groupPrefix + "ksql-queries",
        "The current error status of the given query, if the state is in ERROR state",
        ImmutableMap.of("status", "app-id"));
  }

  @Test
  public void shouldInitiallyHaveInitialState() {
    // When:
    // Listener created in setup

    // Then:
    assertThat(currentGaugeValue(METRIC_NAME_1), is("-"));
    assertThat(currentGaugeValue(METRIC_NAME_2), is("NO_ERROR"));
  }

  @Test
  public void shouldUpdateToNewState() {
    // When:
    listener.onChange(State.REBALANCING, State.RUNNING);

    // Then:
    assertThat(currentGaugeValue(METRIC_NAME_1), is("REBALANCING"));
  }

  @Test
  public void shouldUpdateOnError() {
    // When:
    listener.onError(new QueryError(1, "foo", Type.USER));

    // Then:
    assertThat(currentGaugeValue(METRIC_NAME_2), is("USER"));
  }

  @Test
  public void shouldRemoveMetricOnClose() {
    // When:
    listener.close();

    // Then:
    verify(metrics).removeMetric(METRIC_NAME_1);
    verify(metrics).removeMetric(METRIC_NAME_2);
  }

  @Test
  public void shouldResetUptime() {
    // Given:
    final Multimap<State, State> states = ArrayListMultimap.create();

    states.put(State.CREATED, State.RUNNING);
    states.put(State.CREATED, State.REBALANCING);
    states.put(State.ERROR, State.RUNNING);
    states.put(State.ERROR, State.REBALANCING);

    // These states transitions are not valid for Kafka streams, but KSQL does not know about it
    states.put(State.NOT_RUNNING, State.RUNNING);
    states.put(State.NOT_RUNNING, State.REBALANCING);
    states.put(State.PENDING_SHUTDOWN, State.RUNNING);
    states.put(State.PENDING_SHUTDOWN, State.REBALANCING);

    final long tickForUptime = states.size();
    long nextTick = 1;
    for (final Map.Entry<State, State> stateTransition : states.entries()) {
      final State from = stateTransition.getKey();
      final State to = stateTransition.getValue();

      // When:
      when(ticker.read()).thenReturn(nextTick++).thenReturn(tickForUptime);
      listener.onChange(to, from);

      // Then:
      verify(ticker).read();
      assertThat(listener.uptime(), is(tickForUptime - nextTick + 1));

      reset(ticker);
    }
  }

  @Test
  public void shouldNotResetUptime() {
    // Given:
    final Multimap<State, State> states = ArrayListMultimap.create();
    states.put(State.RUNNING, State.REBALANCING);
    states.put(State.REBALANCING, State.RUNNING);
    states.put(State.RUNNING, State.RUNNING);
    states.put(State.REBALANCING, State.REBALANCING);

    // Set the initial time, and verify it won't change
    when(ticker.read()).thenReturn(SOME_TIME);
    listener.onChange(State.RUNNING, State.CREATED);
    reset(ticker);

    final long tickForUptime = states.size();
    for (final Map.Entry<State, State> stateTransition : states.entries()) {
      final State from = stateTransition.getKey();
      final State to = stateTransition.getValue();

      // When:
      listener.onChange(to, from);

      // Then:
      verify(ticker, never()).read();
      when(ticker.read()).thenReturn(tickForUptime);
      assertThat(listener.uptime(), is(tickForUptime - SOME_TIME));

      reset(ticker);
    }
  }

  @Test
  public void shouldReturnZeroUptime() {
    // Given:
    final Multimap<State, State> states = ArrayListMultimap.create();
    states.put(State.RUNNING, State.ERROR);
    states.put(State.REBALANCING, State.ERROR);
    states.put(State.RUNNING, State.PENDING_SHUTDOWN);
    states.put(State.REBALANCING, State.PENDING_SHUTDOWN);
    states.put(State.RUNNING, State.NOT_RUNNING);
    states.put(State.REBALANCING, State.NOT_RUNNING);
    states.put(State.RUNNING, State.CREATED);
    states.put(State.REBALANCING, State.CREATED);

    for (final Map.Entry<State, State> stateTransition : states.entries()) {
      final State from = stateTransition.getKey();
      final State to = stateTransition.getValue();

      // When:
      listener.onChange(to, from);

      // Then:
      verify(ticker, never()).read();
      assertThat(listener.uptime(), is(0L));
    }
  }

  private String currentGaugeValue(final MetricName name) {
    verify(metrics).addMetric(eq(name), gaugeCaptor.capture());
    return gaugeCaptor.getValue().value(null, 0L);
  }
}