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

package io.confluent.ksql.internal;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metrics.MeasurableStat;
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
  private final Map<String, String> legacyCustomMetricsTags;
  private final Map<String, String> customMetricsTags;
  private final String ksqlServiceIdLegacyPrefix;
  private final String ksqlServicePrefix;
  private final Time time;

  public PullQueryExecutorMetrics(
      final String ksqlServiceId,
      final Map<String, String> customMetricsTags,
      final Time time
  ) {
    this.ksqlServiceIdLegacyPrefix = ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX
        + ksqlServiceId;
    this.legacyCustomMetricsTags = Objects.requireNonNull(customMetricsTags, "customMetricsTags");

    this.ksqlServicePrefix = ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX;
    final Map<String, String> metricsTags = new HashMap<>(customMetricsTags);
    metricsTags.put(KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, ksqlServiceId);
    this.customMetricsTags = ImmutableMap.copyOf(metricsTags);

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

  public List<Sensor> getSensors() {
    return sensors;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  private Sensor configureLocalRequestsSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-local");
    
    // legacy
    addSensor(
        sensor,
        PULL_REQUESTS + "-local-count",
        ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
        "Count of local pull query requests",
        legacyCustomMetricsTags,
        new WindowedCount()
    );
    addSensor(
        sensor,
        PULL_REQUESTS + "-local-rate",
        ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
        "Rate of local pull query requests",
        legacyCustomMetricsTags,
        new Rate()
    );

    // new metrics with ksql service id in tags
    addSensor(
        sensor,
        PULL_REQUESTS + "-local-count",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Count of local pull query requests",
        customMetricsTags,
        new WindowedCount()
    );
    addSensor(
        sensor,
        PULL_REQUESTS + "-local-rate",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Rate of local pull query requests",
        customMetricsTags,
        new Rate()
    );
    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureRemoteRequestsSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-remote");

    // legacy
    addSensor(
        sensor,
        PULL_REQUESTS + "-remote-count",
        ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
        "Count of remote pull query requests",
        legacyCustomMetricsTags,
        new WindowedCount()
    );
    addSensor(
        sensor,
        PULL_REQUESTS + "-remote-rate",
        ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
        "Rate of remote pull query requests",
        legacyCustomMetricsTags,
        new Rate()
    );
    
    // new metrics with ksql service in tags
    addSensor(
        sensor,
        PULL_REQUESTS + "-remote-count",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Count of remote pull query requests",
        customMetricsTags,
        new WindowedCount()
    );
    addSensor(
        sensor,
        PULL_REQUESTS + "-remote-rate",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Rate of remote pull query requests",
        customMetricsTags,
        new Rate()
    );

    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureRateSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-rate");
    
    // legacy
    addSensor(
        sensor,
        PULL_REQUESTS + "-rate",
        ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
        "Rate of pull query requests",
        legacyCustomMetricsTags,
        new Rate()
    );

    // new metrics with ksql service id in tags
    addSensor(
        sensor,
        PULL_REQUESTS + "-rate",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Rate of pull query requests",
        customMetricsTags,
        new Rate()
    );
    
    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureErrorRateSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-error-rate");
    // legacy
    addSensor(
        sensor,
        PULL_REQUESTS + "-error-rate",
        ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
        "Rate of erroneous pull query requests",
        legacyCustomMetricsTags,
        new Rate()
    );
    addSensor(
        sensor,
        PULL_REQUESTS + "-error-total",
        ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
        "Total number of erroneous pull query requests",
        legacyCustomMetricsTags,
        new WindowedCount()
    );

    // new metrics with ksql service id in tags
    addSensor(
        sensor,
        PULL_REQUESTS + "-error-rate",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Rate of erroneous pull query requests",
        customMetricsTags,
        new Rate()
    );
    addSensor(
        sensor,
        PULL_REQUESTS + "-error-total",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Total number of erroneous pull query requests",
        customMetricsTags,
        new WindowedCount()
    );

    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureRequestSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-latency");
    // legacy
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-latency-avg",
            ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
            "Average time for a pull query request",
            legacyCustomMetricsTags
        ),
        new Avg()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-latency-max",
            ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
            "Max time for a pull query request",
            legacyCustomMetricsTags
        ),
        new Max()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-latency-min",
            ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
            "Min time for a pull query request",
            legacyCustomMetricsTags
        ),
        new Min()
    );
    sensor.add(
        metrics.metricName(
            PULL_REQUESTS + "-total",
            ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
            "Total number of pull query request",
            legacyCustomMetricsTags
        ),
        new WindowedCount()
    );

    // new metrics with ksql service id in tags
    addSensor(
        sensor,
        PULL_REQUESTS + "-latency-avg",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Average time for a pull query request",
        customMetricsTags,
        new Avg()
    );
    addSensor(
        sensor,
        PULL_REQUESTS + "-latency-max",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Max time for a pull query request",
        customMetricsTags,
        new Max()
    );
    addSensor(
        sensor,
        PULL_REQUESTS + "-latency-min",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Min time for a pull query request",
        customMetricsTags,
        new Min()
    );
    addSensor(
        sensor,
        PULL_REQUESTS + "-total",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Total number of pull query request",
        customMetricsTags,
        new WindowedCount()
    );

    // legacy percentiles
    sensor.add(new Percentiles(
        100,
        0,
        1000,
        BucketSizing.CONSTANT,
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-50",
            ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
            "Latency distribution",
            legacyCustomMetricsTags
        ), 50.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-75",
            ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
            "Latency distribution",
            legacyCustomMetricsTags
        ), 75.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-90",
            ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
            "Latency distribution",
            legacyCustomMetricsTags
        ), 90.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-99",
            ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
            "Latency distribution",
            legacyCustomMetricsTags
        ), 99.0)
        ));
    
    // new percentile metrics with ksql service id in tag
    sensor.add(new Percentiles(
        100,
        0,
        1000,
        BucketSizing.CONSTANT,
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-50",
            ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
            "Latency distribution",
            customMetricsTags
        ), 50.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-75",
            ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
            "Latency distribution",
            customMetricsTags
        ), 75.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-90",
            ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
            "Latency distribution",
            customMetricsTags
        ), 90.0),
        new Percentile(metrics.metricName(
            PULL_REQUESTS + "-distribution-99",
            ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
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
    // legacy
    addSensor(
        sensor,
        PULL_REQUESTS + "-request-size",
        ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
        "Size in bytes of pull query request",
        legacyCustomMetricsTags,
        new CumulativeSum()
    );

    // new metrics with ksql service id in tags
    addSensor(
        sensor,
        PULL_REQUESTS + "-request-size",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Size in bytes of pull query request",
        customMetricsTags,
        new CumulativeSum()
    );

    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureResponseSizeSensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-response-size");
    // legacy
    addSensor(
        sensor,
        PULL_REQUESTS + "-response-size",
        ksqlServiceIdLegacyPrefix + PULL_QUERY_METRIC_GROUP,
        "Size in bytes of pull query response",
        legacyCustomMetricsTags,
        new CumulativeSum()
    );

    // new metrics with ksql service id in tags
    addSensor(
        sensor,
        PULL_REQUESTS + "-response-size",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Size in bytes of pull query response",
        customMetricsTags,
        new CumulativeSum()
    );

    sensors.add(sensor);
    return sensor;
  }

  private void addSensor(
          final Sensor sensor,
          final String metricName,
          final String groupName,
          final String description,
          final Map<String, String> metricsTags,
          final MeasurableStat measureableStat
  ) {
    sensor.add(
        metrics.metricName(
            metricName,
            groupName,
            description,
            metricsTags
        ),
        measureableStat
    );
  }
}
