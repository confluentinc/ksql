/*
 * Copyright 2021 Confluent Inc.
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.internal.MetricsReporter.DataPoint;
import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class JmxDataPointsReporterTest {
  private static final String GROUP = "group";
  private static final Duration STALE_THRESHOLD = Duration.ofMinutes(1);
  private static final Instant A_TIME = Instant.now();

  @Mock
  private Metrics metrics;
  @Mock
  private MetricName metricName;
  @Captor
  private ArgumentCaptor<Gauge<?>> metricCaptor;
  private JmxDataPointsReporter reporter;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    when(metrics.metricName(any(), any(), anyMap())).thenReturn(metricName);
    reporter = new JmxDataPointsReporter(metrics, GROUP, STALE_THRESHOLD);
  }

  @Test
  public void shouldAddGaugeForNewPoint() {
    // When:
    reporter.report(ImmutableList.of(
        new DataPoint(
            A_TIME,
            "baz",
            123,
            ImmutableMap.of("foo", "bar")
        )
    ));

    // Then:
    verify(metrics).addMetric(same(metricName), metricCaptor.capture());
    assertThat(metricCaptor.getValue().value(null, A_TIME.toEpochMilli()), is(123));
  }

  @Test
  public void shouldUpdateGauge() {
    // Given:
    reporter.report(ImmutableList.of(
        new DataPoint(
            A_TIME,
            "baz",
            123,
            ImmutableMap.of("foo", "bar")
        )
    ));

    // When:
    reporter.report(ImmutableList.of(
        new DataPoint(
            A_TIME.plusSeconds(1),
            "baz",
            456,
            ImmutableMap.of("foo", "bar")
        )
    ));

    // Then:
    verify(metrics).addMetric(same(metricName), metricCaptor.capture());
    assertThat(
        metricCaptor.getValue().value(null, A_TIME.plusSeconds(1).toEpochMilli()),
        is(456)
    );
  }

  @Test
  public void shouldCleanup() {
    // Given:
    reporter.report(ImmutableList.of(
        new DataPoint(
            A_TIME,
            "baz",
            123,
            ImmutableMap.of("foo", "bar")
        )
    ));

    // When:
    reporter.cleanup("baz", ImmutableMap.of("foo", "bar"));

    // Then:
    verify(metrics).removeMetric(metricName);
  }

  @Test
  public void shouldReturnNullForStalePoint() {
    // When:
    reporter.report(ImmutableList.of(
        new DataPoint(
            A_TIME,
            "baz",
            123,
            ImmutableMap.of("foo", "bar")
        )
    ));

    // Then:
    verify(metrics).addMetric(same(metricName), metricCaptor.capture());
    assertThat(
        metricCaptor.getValue().value(
            null,
            A_TIME.plus(STALE_THRESHOLD.multipliedBy(2)).toEpochMilli()),
        nullValue()
    );
  }
}