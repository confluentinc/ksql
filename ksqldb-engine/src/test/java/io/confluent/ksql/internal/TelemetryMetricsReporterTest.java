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

import io.confluent.ksql.internal.MetricsReporter.DataPoint;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.emitter.Emitter;
import io.confluent.telemetry.metrics.SinglePointMetric;
import io.confluent.telemetry.provider.KsqlProvider;
import io.confluent.telemetry.provider.ProviderRegistry;
import io.confluent.telemetry.reporter.TelemetryReporter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TelemetryMetricsReporterTest {

  private static final String METRIC_GROUP = "test-group";
  private static final String PROVIDER_NAMESPACE = "test-namespace";
  private static final String RESOURCE_TYPE = "test-resource-type";
  private static final Map<String, ?> TELEMETRY_REPORTER_CONFIG = mkMap(mkEntry("config1", "value1"));

  private TelemetryMetricsReporter reporter;

  @Mock
  private TelemetryReporter telemetryReporter;
  @Mock
  private Emitter emitter;

  @Before
  public void setUp() {
    when(telemetryReporter.emitter()).thenReturn(emitter);
    reporter = new TelemetryMetricsReporter(
            METRIC_GROUP,
            PROVIDER_NAMESPACE,
            RESOURCE_TYPE,
            TELEMETRY_REPORTER_CONFIG,
            telemetryReporter
    );
  }

  @Test
  public void shouldConfigureTelemetryReporterOnCreation() {
    // reporter is created in setUp()
    assertThat(ProviderRegistry.getProvider(PROVIDER_NAMESPACE), instanceOf(KsqlProvider.class));
    verify(telemetryReporter, times(1)).configure(TELEMETRY_REPORTER_CONFIG);
  }

  @Test
  public void shouldReportDataPoint() {
    final Instant timestamp1 = Instant.now();
    final String metricName1 = "metric1";
    final double value1 = 2.4;
    final Map<String, String> tags1 = mkMap(mkEntry("label1", "value2"), mkEntry("label3", "value4"));
    final Instant timestamp2 = Instant.now();
    final String metricName2 = "metric2";
    final double value2 = 4.2;
    final Map<String, String> tags2 = mkMap(mkEntry("labelA", "valueB"), mkEntry("labelC", "valueD"));
    final List<DataPoint> dataPoints = Arrays.asList(
            new DataPoint(timestamp1, metricName1, value1, tags1),
            new DataPoint(timestamp2, metricName2, value2, tags2)
    );

    // since SinglePointMetric does not override equals(),
    // we need to make a more convoluted verification
    final MetricKey metricKey1 = new MetricKey(fullMetricName(metricName1), tags1);
    final MetricKey metricKey2 = new MetricKey(fullMetricName(metricName2), tags2);
    final SinglePointMetric expectedSinglePointMetric1 = SinglePointMetric.gauge(metricKey1, value1, timestamp1);
    final SinglePointMetric expectedSinglePointMetric2 = SinglePointMetric.gauge(metricKey2, value2, timestamp2);

    try (MockedStatic<SinglePointMetric> mocked = mockStatic(SinglePointMetric.class)) {
      // verify that SinglePointMetric are constructed with the correct arguments
      mocked.when(() -> SinglePointMetric.gauge(metricKey1, value1, timestamp1))
              .thenReturn(expectedSinglePointMetric1);
      mocked.when(() -> SinglePointMetric.gauge(metricKey2, value2, timestamp2))
              .thenReturn(expectedSinglePointMetric2);

      reporter.report(dataPoints);

      // verify that the metric are emitted and no nulls are emitted
      verify(emitter, times(0)).emitMetric(null);
      verify(emitter, times(2)).emitMetric(any(SinglePointMetric.class));
    }
  }

  @Test
  public void shouldCloseTelemetryReporterOnClose() {
    reporter.close();

    verify(telemetryReporter, times(1)).close();
  }

  private String fullMetricName(final String metricName) {
    return MetricsUtils.fullMetricName(KsqlProvider.DOMAIN, METRIC_GROUP, metricName);
  }
}