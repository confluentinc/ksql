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
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullSourceType;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.RoutingNodeType;
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
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class PullQueryExecutorMetrics implements Closeable {

  private static final String PULL_QUERY_METRIC_GROUP = "pull-query";
  private static final String PULL_REQUESTS = "pull-query-requests";

  private final List<Sensor> sensors;
  private final Sensor localRequestsSensor;
  private final Sensor remoteRequestsSensor;
  private final Sensor latencySensor;
  private final Map<MetricsKey, Sensor> latencySensorMap;
  private final Sensor requestRateSensor;
  private final Sensor errorRateSensor;
  private final Map<MetricsKey, Sensor> errorRateSensorMap;
  private final Sensor requestSizeSensor;
  private final Sensor responseSizeSensor;
  private final Map<MetricsKey, Sensor> responseSizeSensorMap;
  private final Sensor responseCode2XX;
  private final Sensor responseCode3XX;
  private final Sensor responseCode4XX;
  private final Sensor responseCode5XX;
  private final Map<MetricsKey, Sensor> rowsReturnedSensorMap;
  private final Map<MetricsKey, Sensor> rowsProcessedSensorMap;
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
    this.latencySensor = configureLatencySensor();
    this.latencySensorMap = configureLatencySensorMap();
    this.requestRateSensor = configureRateSensor();
    this.errorRateSensor = configureErrorRateSensor();
    this.errorRateSensorMap = configureErrorSensorMap();
    this.requestSizeSensor = configureRequestSizeSensor();
    this.responseSizeSensor = configureResponseSizeSensor();
    this.responseSizeSensorMap = configureResponseSizeSensorMap();
    this.responseCode2XX = configureStatusCodeSensor("2XX");
    this.responseCode3XX = configureStatusCodeSensor("3XX");
    this.responseCode4XX = configureStatusCodeSensor("4XX");
    this.responseCode5XX = configureStatusCodeSensor("5XX");
    this.rowsReturnedSensorMap = configureRowsReturnedSensorMap();
    this.rowsProcessedSensorMap = configureRowsProcessedSensorMap();
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

  public void recordLatency(
      final long startTimeNanos,
      final PullSourceType sourceType,
      final PullPhysicalPlanType planType,
      final RoutingNodeType routingNodeType
  ) {
    // Record latency at microsecond scale
    final long nowNanos = time.nanoseconds();
    final double latency = TimeUnit.NANOSECONDS.toMicros(nowNanos - startTimeNanos);
    this.latencySensor.record(latency);
    this.requestRateSensor.record(1);
    final MetricsKey key = new MetricsKey(sourceType, planType, routingNodeType);
    if (latencySensorMap.containsKey(key)) {
      latencySensorMap.get(key).record(latency);
    } else {
      throw new IllegalStateException("Metrics not configured correctly, missing " + key);
    }
  }

  public void recordErrorRate(
      final double value,
      final PullSourceType sourceType,
      final PullPhysicalPlanType planType,
      final RoutingNodeType routingNodeType
  ) {
    this.errorRateSensor.record(value);
    final MetricsKey key = new MetricsKey(sourceType, planType, routingNodeType);
    if (errorRateSensorMap.containsKey(key)) {
      errorRateSensorMap.get(key).record(value);
    } else {
      throw new IllegalStateException("Metrics not configured correctly, missing " + key);
    }
  }

  public void recordRequestSize(final double value) {
    this.requestSizeSensor.record(value);
  }

  public void recordResponseSize(
      final double value,
      final PullSourceType sourceType,
      final PullPhysicalPlanType planType,
      final RoutingNodeType routingNodeType
  ) {
    this.responseSizeSensor.record(value);
    final MetricsKey key = new MetricsKey(sourceType, planType, routingNodeType);
    if (responseSizeSensorMap.containsKey(key)) {
      responseSizeSensorMap.get(key).record(value);
    } else {
      throw new IllegalStateException("Metrics not configured correctly, missing " + key);
    }
  }

  public void recordStatusCode(final int statusCode) {
    if (statusCode >= 200 && statusCode < 300) {
      responseCode2XX.record(1);
    } else if (statusCode >= 300 && statusCode < 400) {
      responseCode3XX.record(1);
    } else if (statusCode >= 400 && statusCode < 500) {
      responseCode4XX.record(1);
    } else if (statusCode >= 500) {
      responseCode5XX.record(1);
    }
  }

  public void recordRowsReturned(
      final double value,
      final PullSourceType sourceType,
      final PullPhysicalPlanType planType,
      final RoutingNodeType routingNodeType
  ) {
    final MetricsKey key = new MetricsKey(sourceType, planType, routingNodeType);
    if (rowsReturnedSensorMap.containsKey(key)) {
      rowsReturnedSensorMap.get(key).record(value);
    } else {
      throw new IllegalStateException("Metrics not configured correctly, missing " + key);
    }
  }

  public void recordRowsProcessed(
      final double value,
      final PullSourceType sourceType,
      final PullPhysicalPlanType planType,
      final RoutingNodeType routingNodeType
  ) {
    final MetricsKey key = new MetricsKey(sourceType, planType, routingNodeType);
    if (rowsProcessedSensorMap.containsKey(key)) {
      rowsProcessedSensorMap.get(key).record(value);
    } else {
      throw new IllegalStateException("Metrics not configured correctly, missing " + key);
    }
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
        new CumulativeCount()
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
        new CumulativeCount()
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
        new CumulativeCount()
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
        new CumulativeCount()
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
        new CumulativeCount()
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
        new CumulativeCount()
    );

    sensors.add(sensor);
    return sensor;
  }

  private Map<MetricsKey, Sensor> configureErrorSensorMap() {
    return configureSensorMap("error", (sensor, tags, variantName) -> {
      addSensor(
          sensor,
          PULL_REQUESTS + "-detailed-error-total",
          ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
          "Total number of erroneous pull query requests - " + variantName,
          tags,
          new CumulativeCount()
      );
    });
  }

  private Sensor configureStatusCodeSensor(final String codeName) {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-" + codeName + "-total");
    addSensor(
        sensor,
        PULL_REQUESTS + "-" + codeName + "-total",
        ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
        "Total number of status code " + codeName + " responses",
        customMetricsTags,
        new CumulativeCount()
    );

    sensors.add(sensor);
    return sensor;
  }

  private Sensor configureLatencySensor() {
    final Sensor sensor = metrics.sensor(
        PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-latency");

    // Legacy metrics
    addRequestMetricsToSensor(sensor, ksqlServiceIdLegacyPrefix, PULL_REQUESTS,
        legacyCustomMetricsTags, "");

    // New metrics
    addRequestMetricsToSensor(sensor, ksqlServicePrefix, PULL_REQUESTS, customMetricsTags, "");

    sensors.add(sensor);
    return sensor;
  }

  private Map<MetricsKey, Sensor> configureLatencySensorMap() {
    return configureSensorMap("latency", (sensor, tags, variantName) -> {
      addRequestMetricsToSensor(sensor, ksqlServicePrefix, PULL_REQUESTS + "-detailed",
          tags, " - " + variantName);
    });
  }

  private void addRequestMetricsToSensor(
      final Sensor sensor,
      final String servicePrefix,
      final String metricNamePrefix,
      final Map<String, String> metricsTags,
      final String descriptionSuffix
  ) {
    addSensor(
        sensor,
        metricNamePrefix + "-latency-avg",
        servicePrefix + PULL_QUERY_METRIC_GROUP,
        "Average time for a pull query request" + descriptionSuffix,
        metricsTags,
        new Avg()
    );
    addSensor(
        sensor,
        metricNamePrefix + "-latency-max",
        servicePrefix + PULL_QUERY_METRIC_GROUP,
        "Max time for a pull query request" + descriptionSuffix,
        metricsTags,
        new Max()
    );
    addSensor(
        sensor,
        metricNamePrefix + "-latency-min",
        servicePrefix + PULL_QUERY_METRIC_GROUP,
        "Min time for a pull query request" + descriptionSuffix,
        metricsTags,
        new Min()
    );
    addSensor(
        sensor,
        metricNamePrefix + "-total",
        servicePrefix + PULL_QUERY_METRIC_GROUP,
        "Total number of pull query request" + descriptionSuffix,
        metricsTags,
        new CumulativeCount()
    );

    sensor.add(new Percentiles(
        100,
        0,
        1000,
        BucketSizing.CONSTANT,
        new Percentile(metrics.metricName(
            metricNamePrefix + "-distribution-50",
            servicePrefix + PULL_QUERY_METRIC_GROUP,
            "Latency distribution" + descriptionSuffix,
            metricsTags
        ), 50.0),
        new Percentile(metrics.metricName(
            metricNamePrefix + "-distribution-75",
            servicePrefix + PULL_QUERY_METRIC_GROUP,
            "Latency distribution" + descriptionSuffix,
            metricsTags
        ), 75.0),
        new Percentile(metrics.metricName(
            metricNamePrefix + "-distribution-90",
            servicePrefix + PULL_QUERY_METRIC_GROUP,
            "Latency distribution" + descriptionSuffix,
            metricsTags
        ), 90.0),
        new Percentile(metrics.metricName(
            metricNamePrefix + "-distribution-99",
            servicePrefix + PULL_QUERY_METRIC_GROUP,
            "Latency distribution" + descriptionSuffix,
            metricsTags
        ), 99.0)
    ));
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

  private Map<MetricsKey, Sensor> configureResponseSizeSensorMap() {
    return configureSensorMap("response-size", (sensor, tags, variantName) -> {
      addSensor(
          sensor,
          PULL_REQUESTS + "-detailed-response-size",
          ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
          "Size in bytes of pull query response - " + variantName,
          tags,
          new CumulativeSum()
      );
    });
  }

  private Map<MetricsKey, Sensor> configureRowsReturnedSensorMap() {
    return configureSensorMap("rows-returned", (sensor, tags, variantName) -> {
      addSensor(
          sensor,
          PULL_REQUESTS + "-rows-returned-total",
          ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
          "Number of rows returned - " + variantName,
          tags,
          new CumulativeSum()
      );
    });
  }

  private Map<MetricsKey, Sensor> configureRowsProcessedSensorMap() {
    return configureSensorMap("rows-processed", (sensor, tags, variantName) -> {
      addSensor(
          sensor,
          PULL_REQUESTS + "-rows-processed-total",
          ksqlServicePrefix + PULL_QUERY_METRIC_GROUP,
          "Number of rows processed -" + variantName,
          tags,
          new CumulativeSum()
      );
    });
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

  private Map<MetricsKey, Sensor> configureSensorMap(
      final String sensorBaseName, final MetricsAdder metricsAdder) {
    final ImmutableMap.Builder<MetricsKey, Sensor> builder
        = ImmutableMap.builder();

    for (final PullSourceType sourceType : PullSourceType.values()) {
      for (final PullPhysicalPlanType planType : PullPhysicalPlanType.values()) {
        for (final RoutingNodeType routingNodeType : RoutingNodeType.values()) {
          final String variantName = sourceType.name().toLowerCase() + "-"
              + planType.name().toLowerCase() + "-" + routingNodeType.name().toLowerCase();
          final Sensor sensor = metrics.sensor(
              PULL_QUERY_METRIC_GROUP + "-" + PULL_REQUESTS + "-" + sensorBaseName + "-"
                  + variantName);

          final ImmutableMap<String, String> tags = ImmutableMap.<String, String>builder()
              .putAll(customMetricsTags)
              .put(KsqlConstants.KSQL_QUERY_SOURCE_TAG, sourceType.name().toLowerCase())
              .put(KsqlConstants.KSQL_QUERY_PLAN_TYPE_TAG, planType.name().toLowerCase())
              .put(KsqlConstants.KSQL_QUERY_ROUTING_TYPE_TAG,
                  routingNodeType.name().toLowerCase())
              .build();

          metricsAdder.addMetrics(sensor, tags, variantName);

          builder.put(new MetricsKey(sourceType, planType, routingNodeType), sensor);
          sensors.add(sensor);
        }
      }
    }

    return builder.build();
  }

  private interface MetricsAdder {
    void addMetrics(Sensor sensor, Map<String, String> tags, String variantName);
  }

  // Detailed metrics are broken down by multiple parameters represented by the following key.
  private static class MetricsKey {

    private final PullSourceType sourceType;
    private final PullPhysicalPlanType planType;
    private final RoutingNodeType routingNodeType;

    MetricsKey(
        final PullSourceType sourceType,
        final PullPhysicalPlanType planType,
        final RoutingNodeType routingNodeType
    ) {
      this.sourceType = sourceType;
      this.planType = planType;
      this.routingNodeType = routingNodeType;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final MetricsKey key = (MetricsKey) o;
      return Objects.equals(sourceType, key.sourceType)
          && Objects.equals(planType, key.planType)
          && Objects.equals(routingNodeType, key.routingNodeType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceType, planType, routingNodeType);
    }

    @Override
    public String toString() {
      return "MetricsKey{"
          + "sourceType=" + sourceType
          + ", planType=" + planType
          + ", routingNodeType=" + routingNodeType
          + '}';
    }
  }
}
