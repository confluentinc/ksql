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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;

public class JmxDataPointsReporter implements MetricsReporter {
  private final Metrics metrics;
  private final String group;
  private final Map<MetricName, DataPointBasedGauge> gauges = new ConcurrentHashMap<>();
  private final Duration staleThreshold;

  public JmxDataPointsReporter(
      final Metrics metrics,
      final String group,
      final Duration staleThreshold
  ) {
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.group = Objects.requireNonNull(group, "group");
    this.staleThreshold = Objects.requireNonNull(staleThreshold, "staleThreshold");
  }

  @Override
  public void report(final List<DataPoint> dataPoints) {
    dataPoints.forEach(this::report);
  }

  private void report(final DataPoint dataPoint) {
    final MetricName metricName
        = metrics.metricName(dataPoint.getName(), group, dataPoint.getTags());
    // Fast path: gauge already exists, just refresh the data point.
    final DataPointBasedGauge existing = gauges.get(metricName);
    if (existing != null) {
      existing.dataPointRef.set(dataPoint);
      return;
    }
    // Slow path: install a new gauge. Use putIfAbsent so concurrent reporters racing
    // on the same metric name don't both call metrics.addMetric (which would throw
    // "metric already exists") or both overwrite the gauge reference. Whichever
    // thread wins putIfAbsent registers with the metrics registry exactly once; the
    // losers update the winning gauge's data point.
    final DataPointBasedGauge created = new DataPointBasedGauge(dataPoint, staleThreshold);
    final DataPointBasedGauge winner = gauges.putIfAbsent(metricName, created);
    if (winner == null) {
      metrics.addMetric(metricName, created);
    } else {
      winner.dataPointRef.set(dataPoint);
    }
  }

  @Override
  public void cleanup(final String name, final Map<String, String> tags) {
    final MetricName metricName = metrics.metricName(name, group, tags);
    metrics.removeMetric(metricName);
    gauges.remove(metricName);
  }

  @Override
  public void close() {
  }

  @Override
  public void configure(final Map<String, ?> map) {
  }

  private static final class DataPointBasedGauge implements Gauge<Object> {
    private final AtomicReference<DataPoint> dataPointRef;
    private final Duration staleThreshold;

    private DataPointBasedGauge(
        final DataPoint initial,
        final Duration staleThreshold
    ) {
      this.dataPointRef = new AtomicReference<>(initial);
      this.staleThreshold = staleThreshold;
    }

    @Override
    public Object value(final MetricConfig metricConfig, final long now) {
      final DataPoint dataPoint = dataPointRef.get();
      if (dataPoint.getTime().isAfter(Instant.ofEpochMilli(now).minus(staleThreshold))) {
        return dataPoint.getValue();
      }
      return null;
    }
  }
}
