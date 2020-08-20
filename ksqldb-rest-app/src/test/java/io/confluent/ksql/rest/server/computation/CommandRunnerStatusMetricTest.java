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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class CommandRunnerStatusMetricTest {

  private static final MetricName METRIC_NAME =
      new MetricName("bob", "g1", "d1", ImmutableMap.of());
  private static final String KSQL_SERVICE_ID = "kcql-1-";

  @Mock
  private Metrics metrics;
  @Mock
  private CommandRunner commandRunner;
  @Captor
  private ArgumentCaptor<Gauge<String>> gaugeCaptor;

  private CommandRunnerStatusMetric commandRunnerStatusMetric;

  @Before
  public void setUp() {
    when(metrics.metricName(any(), any(), any(), anyMap())).thenReturn(METRIC_NAME);
    when(commandRunner.checkCommandRunnerStatus()).thenReturn(CommandRunner.CommandRunnerStatus.RUNNING);

    commandRunnerStatusMetric = new CommandRunnerStatusMetric(metrics, commandRunner, KSQL_SERVICE_ID, "rest");
  }

  @Test
  public void shouldAddMetricOnCreation() {
    // When:
    // Listener created in setup

    // Then:
    verify(metrics).metricName("status", "_confluent-ksql-kcql-1-rest-command-runner",
            "The status of the commandRunner thread as it processes the command topic.",
            Collections.emptyMap());

    verify(metrics).addMetric(eq(METRIC_NAME), isA(Gauge.class));
  }

  @Test
  public void shouldInitiallyBeRunningState() {
    // When:
    // CommandRunnerStatusMetric created in setup

    // Then:
    assertThat(currentGaugeValue(), is(CommandRunner.CommandRunnerStatus.RUNNING.name()));
  }

  @Test
  public void shouldUpdateToErrorState() {
    // When:
    when(commandRunner.checkCommandRunnerStatus()).thenReturn(CommandRunner.CommandRunnerStatus.ERROR);

    // Then:
    assertThat(currentGaugeValue(), is(CommandRunner.CommandRunnerStatus.ERROR.name()));
  }

  @Test
  public void shouldUpdateToDegradedState() {
    // When:
    when(commandRunner.checkCommandRunnerStatus()).thenReturn(CommandRunner.CommandRunnerStatus.DEGRADED);

    // Then:
    assertThat(currentGaugeValue(), is(CommandRunner.CommandRunnerStatus.DEGRADED.name()));
  }

  @Test
  public void shouldRemoveMetricOnClose() {
    // When:
    commandRunnerStatusMetric.close();

    // Then:
    verify(metrics).removeMetric(METRIC_NAME);
  }

  private String currentGaugeValue() {
    verify(metrics).addMetric(any(), gaugeCaptor.capture());
    return gaugeCaptor.getValue().value(null, 0L);
  }
}
