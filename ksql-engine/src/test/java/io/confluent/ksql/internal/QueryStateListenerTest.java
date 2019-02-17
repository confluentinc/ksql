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

import static com.google.common.testing.NullPointerTester.Visibility.PACKAGE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
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

@RunWith(MockitoJUnitRunner.class)
public class QueryStateListenerTest {

  private static final MetricName METRIC_NAME =
      new MetricName("bob", "g1", "d1", ImmutableMap.of());

  @Mock
  private Metrics metrics;
  @Captor
  private ArgumentCaptor<Gauge<String>> gaugeCaptor;
  private QueryStateListener listener;

  @Before
  public void setUp() {
    when(metrics.metricName(any(), any(), any(), anyMap())).thenReturn(METRIC_NAME);

    listener = new QueryStateListener(metrics, "app-id");
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

    verify(metrics).addMetric(eq(METRIC_NAME), isA(Gauge.class));
  }

  @Test
  public void shouldInitiallyHaveInitialState() {
    // When:
    // Listener created in setup

    // Then:
    assertThat(currentGaugeValue(), is("-"));
  }

  @Test
  public void shouldUpdateToNewState() {
    // When:
    listener.onChange(State.REBALANCING, State.RUNNING);

    // Then:
    assertThat(currentGaugeValue(), is("REBALANCING"));
  }

  @Test
  public void shouldRemoveMetricOnClose() {
    // When:
    listener.close();

    // Then:
    verify(metrics).removeMetric(METRIC_NAME);
  }

  private String currentGaugeValue() {
    verify(metrics).addMetric(any(), gaugeCaptor.capture());
    return gaugeCaptor.getValue().value(null, 0L);
  }
}