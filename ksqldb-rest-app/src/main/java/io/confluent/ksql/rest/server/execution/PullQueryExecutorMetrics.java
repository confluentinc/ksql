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

import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.Time;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class PullQueryExecutorMetrics implements Closeable {

  private static final String PULL_QUERY_METRIC_GROUP = "pull-query";
  private static final String PULL_REQUESTS = "pull-query-requests";

  private final List<Sensor> sensors;
  private final Sensor localRequestsSensor;
  private final Sensor remoteRequestsSensor;
  private final Sensor latencySensor;
  private final Sensor requestRateSensor;
  private final Sensor errorRateSensor;
  private final Sensor requestSizeSensor;
  private final Sensor responseSizeSensor;
  private final Metrics metrics;
  private final Map<String, String> customMetricsTags;
  private final String ksqlServiceId;
  private final Time time;

  public PullQueryExecutorMetrics(
      final String ksqlServiceId,
      final Map<String, String> customMetricsTags,
      final Time time
  ) {
    this.ksqlServiceId = ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX
        + ksqlServiceId;
    this.customMetricsTags = Objects.requireNonNull(customMetricsTags, "customMetricsTags");
    this.time = Objects.requireNonNull(time, "time");
    this.metrics = MetricCollectors.getMetrics();
    this.sensors = new ArrayList<>();
    this.localRequestsSensor = configureLocalRequestsSensor();
    this.remoteRequestsSensor = configureRemoteRequestsSensor();
    this.latencySensor = configureRequestSensor();
    this.requestRateSensor = configureRateSensor();
    this.errorRateSensor = configureErrorRateSensor();
    this.requestSizeSensor = configureRequestSizeSensor();
    this.responseSizeSensor = configureResponseSizeSensor();
  }

  @Override
  public void close() {
    sensors.forEach(sensor -> metrics.removeSensor(sensor.name()));
  }

  public void recordLocalRequests(final double value) {
    this.localRequestsSensor.record(value);
  }

  public void recordRemoteRequests(final double value) {
    this.remoteRequestsSensor.record(value);
  }

  public void recordLatency(final long startTimeNanos) {
    // Record latency at microsecond scale
    final long nowNanos = time.nanoseconds();
    final double latency = TimeUnit.NANOSECONDS.toMicros(nowNanos - startTimeNanos);
    this.latencySensor.record(latency);
    this.requestRateSensor.record(1);
  }

  public void recordErrorRate(final double value) {
    this.errorRateSensor.record(value);
  }

  public void recordRequestSize(final double value) {
    this.requestSizeSensor.record(value);
  }

  public void recordResponseSize(final double value) {
    this.responseSizeSensor.record(value);
  }

  List<Sensor> getSensors() {
    return sensors;
  }

  Metrics getMetrics() {
    return metrics;
  }

  private Sensor configureLocalRequestsSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-local");
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-local-count",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Count of local pull query requests",
            customMetricsTags
        ),
        new WindowedCount()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-local-rate",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Rate of local pull query requests",
            customMetricsTags
        ),
        new Rate()
    );
    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureRemoteRequestsSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-remote");
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-remote-count",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Count of remote pull query requests",
            customMetricsTags
        ),
        new WindowedCount()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-remote-rate",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Rate of remote pull query requests",
            customMetricsTags
        ),
        new Rate()
    );
    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureRateSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-rate");
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-rate",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Rate of pull query requests",
            customMetricsTags
        ),
        new Rate()
    );
    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureErrorRateSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-error-rate");
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-error-rate",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Rate of erroneous pull query requests",
            customMetricsTags
        ),
        new Rate()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-error-total",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Total number of erroneous pull query requests",
            customMetricsTags
        ),
        new WindowedCount()
    );

    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureRequestSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-latency");
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-latency-avg",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Average time for a pull query request",
            customMetricsTags
        ),
        new Avg()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-latency-max",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Max time for a pull query request",
            customMetricsTags
        ),
        new Max()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-latency-min",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Min time for a pull query request",
            customMetricsTags
        ),
        new Min()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-total",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Total number of pull query request",
            customMetricsTags
        ),
        new WindowedCount()
    );
    sensor.add(new Percentiles(
        100,
        0,
        1000,
        BucketSizing.CONSTANT,
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-50",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Latency distribution",
            customMetricsTags
        ), 50.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-75",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Latency distribution",
            customMetricsTags
        ), 75.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-90",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Latency distribution",
            customMetricsTags
        ), 90.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-99",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Latency distribution",
            customMetricsTags
        ), 99.0)
        ));

    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureRequestSizeSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-request-size");
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-request-size",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Size in bytes of pull query request",
            customMetricsTags
        ),
        new CumulativeSum()
    );

    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureResponseSizeSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-response-size");
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-response-size",
            ksqlServiceId + PULL_QUERY_METRIC_GROUP,
            "Size in bytes of pull query response",
            customMetricsTags
        ),
        new CumulativeSum()
    );

    sensors.add(sensor);
    return sensor;
  }
}
