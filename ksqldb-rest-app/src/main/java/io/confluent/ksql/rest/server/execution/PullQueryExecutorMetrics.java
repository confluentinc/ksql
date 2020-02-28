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
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;

public class PullQueryExecutorMetrics implements Closeable {

  private static final String PULL_QUERY_METRIC_GROUP = "pull-query";
  private static final String PULL_REQUESTS = "pull-query-requests";

  private final List<Sensor> sensors;
  private final Sensor localRequestsSensor;
  private final Sensor remoteRequestsSensor;
  private final Sensor latencySensor;
  private final Sensor requestRateSensor;
  private final Sensor errorRateSensor;
  private final Metrics metrics;

  public PullQueryExecutorMetrics() {
    this.metrics = MetricCollectors.getMetrics();
    this.sensors = new ArrayList<>();
    this.localRequestsSensor = configureLocalRequestsSensor();
    this.remoteRequestsSensor = configureRemoteRequestsSensor();
    this.latencySensor = configureRequestSensor();
    this.requestRateSensor = configureRateSensor();
    this.errorRateSensor = configureErrorRateSensor();
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

  public void recordRate(final double value) {
    this.requestRateSensor.record(value);
  }

  public void recordLatency(final double value) {
    this.latencySensor.record(value);
  }

  public void recordErrorRate(final double value) {
    this.errorRateSensor.record(value);
  }

  private Sensor configureLocalRequestsSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-local");
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-local-count",
            PULL_QUERY_METRIC_GROUP,
            "Count of local pull query requests"
        ),
        new WindowedCount()
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
            PULL_QUERY_METRIC_GROUP,
            "Count of remote pull query requests"
        ),
        new WindowedCount()
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
            PULL_QUERY_METRIC_GROUP,
            "Rate of pull query requests"
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
            PULL_QUERY_METRIC_GROUP,
            "Rate of erroneous pull query requests"
        ),
        new Rate()
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
            PULL_QUERY_METRIC_GROUP,
            "Average time for a pull query request"
        ),
        new Avg()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-latency-max",
            PULL_QUERY_METRIC_GROUP,
            "Max time for a pull query request"
        ),
        new Max()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-latency-min",
            PULL_QUERY_METRIC_GROUP,
            "Min time for a pull query request"
        ),
        new Min()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-total",
            PULL_QUERY_METRIC_GROUP,
            "Total number of pull query request"
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
            PULL_QUERY_METRIC_GROUP,
            "Latency distribution"),
                       50.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-75",
            PULL_QUERY_METRIC_GROUP,
            "Latency distribution"),
                       75.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-90",
            PULL_QUERY_METRIC_GROUP,
            "Latency distribution"),
                       90.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-99",
            PULL_QUERY_METRIC_GROUP,
            "Latency distribution"),
                       99.0)
        ));

    sensors.add(sensor);
    return sensor;
  }
}
