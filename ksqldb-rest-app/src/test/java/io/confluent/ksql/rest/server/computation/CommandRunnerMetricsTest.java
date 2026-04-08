/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;

import io.confluent.ksql.util.KsqlConstants;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandRunnerMetricsTest {

  private static final MetricName METRIC_NAME_1_LEGACY =
      new MetricName("bob", "g1", "d1", ImmutableMap.of());
  private static final MetricName METRIC_NAME_2_LEGACY =
      new MetricName("jill", "g1", "d2", ImmutableMap.of());

  private static final MetricName METRIC_NAME_1 =
      new MetricName("dob", "g1", "d1", ImmutableMap.of());
  private static final MetricName METRIC_NAME_2 =
      new MetricName("bill", "g1", "d2", ImmutableMap.of());
  private static final MetricName NUM_METRIC_NAME_1 =
          new MetricName("n-dob", "g1", "d1", ImmutableMap.of());
  private static final MetricName NUM_METRIC_NAME_2 =
          new MetricName("n-bill", "g1", "d2", ImmutableMap.of());
  private static final String KSQL_SERVICE_ID = "kcql-1-";

  @Mock
  private Metrics metrics;
  @Mock
  private CommandRunner commandRunner;
  @Captor
  private ArgumentCaptor<Gauge<String>> gaugeCaptor;

  @Captor
  private ArgumentCaptor<Gauge<Integer>> gaugeNumCaptor;

  private CommandRunnerMetrics commandRunnerMetrics;

  @Before
  public void setUp() {
    when(metrics.metricName(any(), any(), any(), anyMap()))
        .thenReturn(METRIC_NAME_1_LEGACY)
        .thenReturn(METRIC_NAME_2_LEGACY)
        .thenReturn(METRIC_NAME_1)
        .thenReturn(METRIC_NAME_2)
        .thenReturn(NUM_METRIC_NAME_1)
        .thenReturn(NUM_METRIC_NAME_2);
    when(commandRunner.checkCommandRunnerStatus()).thenReturn(CommandRunner.CommandRunnerStatus.RUNNING);
    when(commandRunner.getCommandRunnerDegradedReason()).thenReturn(CommandRunner.CommandRunnerDegradedReason.NONE);

    commandRunnerMetrics = new CommandRunnerMetrics(metrics, commandRunner, KSQL_SERVICE_ID, "ksql-rest");
  }

  @Test
  public void shouldAddMetricOnCreation() {
    // When:
    // Listener created in setup

    // Then:
    // legacy metrics with ksql service id in the metric name
    final InOrder inOrder = inOrder(metrics);
    inOrder.verify(metrics).metricName("status", "_confluent-ksql-kcql-1-ksql-rest-command-runner",
        "The status of the commandRunner thread as it processes the command topic.",
        Collections.emptyMap());
    inOrder.verify(metrics).metricName("degraded-reason", "_confluent-ksql-kcql-1-ksql-rest-command-runner",
        "The reason for why the commandRunner thread is in a DEGRADED state.",
        Collections.emptyMap());
    
    // new metrics with ksql service id in tags
    inOrder.verify(metrics).metricName("status", "_confluent-ksql-rest-command-runner",
        "The status of the commandRunner thread as it processes the command topic.",
        Collections.singletonMap(KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, KSQL_SERVICE_ID));
    inOrder.verify(metrics).metricName("degraded-reason", "_confluent-ksql-rest-command-runner",
        "The reason for why the commandRunner thread is in a DEGRADED state.",
        Collections.singletonMap(KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, KSQL_SERVICE_ID));

    // same metrics but with num values
    inOrder.verify(metrics).metricName("status-num", "_confluent-ksql-rest-command-runner",
            "The status number of the commandRunner thread as it processes the command topic.",
            Collections.singletonMap(KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, KSQL_SERVICE_ID));
    inOrder.verify(metrics).metricName("degraded-reason-num", "_confluent-ksql-rest-command-runner",
            "The reason number for why the commandRunner thread is in a DEGRADED state.",
            Collections.singletonMap(KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, KSQL_SERVICE_ID));

    inOrder.verify(metrics).addMetric(eq(METRIC_NAME_1_LEGACY), isA(Gauge.class));
    inOrder.verify(metrics).addMetric(eq(METRIC_NAME_2_LEGACY), isA(Gauge.class));
    inOrder.verify(metrics).addMetric(eq(METRIC_NAME_1), isA(Gauge.class));
    inOrder.verify(metrics).addMetric(eq(METRIC_NAME_2), isA(Gauge.class));
    inOrder.verify(metrics).addMetric(eq(NUM_METRIC_NAME_1), isA(Gauge.class));
    inOrder.verify(metrics).addMetric(eq(NUM_METRIC_NAME_2), isA(Gauge.class));
  }

  @Test
  public void shouldInitiallyBeCommandRunnerStatusRunningState() {
    // When:
    // CommandRunnerStatusMetric created in setup

    // Then:
    assertThat(commandRunnerStatusGaugeNumValue(), is(CommandRunner.CommandRunnerStatus.RUNNING.ordinal()));
    assertThat(commandRunnerStatusGaugeValue(), is(CommandRunner.CommandRunnerStatus.RUNNING.name()));
    assertThat(commandRunnerStatusGaugeValueLegacy(), is(CommandRunner.CommandRunnerStatus.RUNNING.name()));
  }

  @Test
  public void shouldUpdateToCommandRunnerStatusErrorState() {
    // When:
    when(commandRunner.checkCommandRunnerStatus()).thenReturn(CommandRunner.CommandRunnerStatus.ERROR);

    // Then:
    assertThat(commandRunnerStatusGaugeNumValue(), is(CommandRunner.CommandRunnerStatus.ERROR.ordinal()));
    assertThat(commandRunnerStatusGaugeValue(), is(CommandRunner.CommandRunnerStatus.ERROR.name()));
    assertThat(commandRunnerStatusGaugeValueLegacy(), is(CommandRunner.CommandRunnerStatus.ERROR.name()));
  }

  @Test
  public void shouldUpdateToCommandRunnerStatusDegradedState() {
    // When:
    when(commandRunner.checkCommandRunnerStatus()).thenReturn(CommandRunner.CommandRunnerStatus.DEGRADED);

    // Then:
    assertThat(commandRunnerStatusGaugeNumValue(), is(CommandRunner.CommandRunnerStatus.DEGRADED.ordinal()));
    assertThat(commandRunnerStatusGaugeValue(), is(CommandRunner.CommandRunnerStatus.DEGRADED.name()));
    assertThat(commandRunnerStatusGaugeValueLegacy(), is(CommandRunner.CommandRunnerStatus.DEGRADED.name()));
  }

  @Test
  public void shouldInitiallyNoneCommandRunnerDegradedReason() {
    // When:
    // CommandRunnerStatusMetric created in setup

    // Then:
    assertThat(commandRunnerDegradedReasonGaugeNumValue(), is(CommandRunner.CommandRunnerDegradedReason.NONE.ordinal()));
    assertThat(commandRunnerDegradedReasonGaugeValue(), is(CommandRunner.CommandRunnerDegradedReason.NONE.name()));
    assertThat(commandRunnerDegradedReasonGaugeValueLegacy(), is(CommandRunner.CommandRunnerDegradedReason.NONE.name()));
  }

  @Test
  public void shouldUpdateToCorruptedCommandRunnerDegradedReason() {
    // When:
    when(commandRunner.getCommandRunnerDegradedReason()).thenReturn(CommandRunner.CommandRunnerDegradedReason.CORRUPTED);

    // Then:
    assertThat(commandRunnerDegradedReasonGaugeNumValue(), is(CommandRunner.CommandRunnerDegradedReason.CORRUPTED.ordinal()));
    assertThat(commandRunnerDegradedReasonGaugeValue(), is(CommandRunner.CommandRunnerDegradedReason.CORRUPTED.name()));
    assertThat(commandRunnerDegradedReasonGaugeValueLegacy(), is(CommandRunner.CommandRunnerDegradedReason.CORRUPTED.name()));
  }

  @Test
  public void shouldUpdateToIncompatibleCommandsCommandRunnerDegradedReason() {
    // When:
    when(commandRunner.getCommandRunnerDegradedReason()).thenReturn(CommandRunner.CommandRunnerDegradedReason.INCOMPATIBLE_COMMAND);

    // Then:
    assertThat(commandRunnerDegradedReasonGaugeNumValue(), is(CommandRunner.CommandRunnerDegradedReason.INCOMPATIBLE_COMMAND.ordinal()));
    assertThat(commandRunnerDegradedReasonGaugeValue(), is(CommandRunner.CommandRunnerDegradedReason.INCOMPATIBLE_COMMAND.name()));
    assertThat(commandRunnerDegradedReasonGaugeValueLegacy(), is(CommandRunner.CommandRunnerDegradedReason.INCOMPATIBLE_COMMAND.name()));
  }

  @Test
  public void shouldRemoveNoneCommandRunnerDegradedReason() {
    // When:
    commandRunnerMetrics.close();

    // Then:
    verify(metrics).removeMetric(METRIC_NAME_1_LEGACY);
    verify(metrics).removeMetric(METRIC_NAME_2_LEGACY);
    verify(metrics).removeMetric(METRIC_NAME_1);
    verify(metrics).removeMetric(METRIC_NAME_2);
    verify(metrics).removeMetric(NUM_METRIC_NAME_1);
    verify(metrics).removeMetric(NUM_METRIC_NAME_2);
  }

  private int commandRunnerStatusGaugeNumValue() {
    verify(metrics).addMetric(eq(NUM_METRIC_NAME_1), gaugeNumCaptor.capture());
    return gaugeNumCaptor.getValue().value(null, 0L);
  }

  private int commandRunnerDegradedReasonGaugeNumValue() {
    verify(metrics).addMetric(eq(NUM_METRIC_NAME_2), gaugeNumCaptor.capture());
    return gaugeNumCaptor.getValue().value(null, 0L);
  }

  private String commandRunnerStatusGaugeValue() {
    verify(metrics).addMetric(eq(METRIC_NAME_1), gaugeCaptor.capture());
    return gaugeCaptor.getValue().value(null, 0L);
  }

  private String commandRunnerDegradedReasonGaugeValue() {
    verify(metrics).addMetric(eq(METRIC_NAME_2), gaugeCaptor.capture());
    return gaugeCaptor.getValue().value(null, 0L);
  }

  private String commandRunnerStatusGaugeValueLegacy() {
    verify(metrics).addMetric(eq(METRIC_NAME_1_LEGACY), gaugeCaptor.capture());
    return gaugeCaptor.getValue().value(null, 0L);
  }

  private String commandRunnerDegradedReasonGaugeValueLegacy() {
    verify(metrics).addMetric(eq(METRIC_NAME_2_LEGACY), gaugeCaptor.capture());
    return gaugeCaptor.getValue().value(null, 0L);
  }
}
