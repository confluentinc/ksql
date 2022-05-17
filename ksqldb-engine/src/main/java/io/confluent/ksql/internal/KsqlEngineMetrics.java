/*
 * Copyright 2018 Confluent Inc.
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

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;

public class KsqlEngineMetrics implements Closeable {

  private static final String DEFAULT_METRIC_GROUP_PREFIX = "ksql-engine";
  private static final String METRIC_GROUP_POST_FIX = "-query-stats";

  private final List<Sensor> sensors;
  private final List<CountMetric> countMetrics;
  private final String metricGroupPrefix;
  private final String metricGroupName;
  private final Sensor messagesIn;
  private final Sensor totalMessagesIn;
  private final Sensor totalBytesIn;
  private final Sensor messagesOut;
  private final Sensor numIdleQueries;
  private final Sensor messageConsumptionByQuery;
  private final Sensor errorRate;

  private final String ksqlServiceIdLegacyPrefix;
  private final String ksqlServicePrefix;
  private final Map<String, String> customMetricsTags;
  private final Map<String, String> newCustomMetricsTags;
  private final Optional<KsqlMetricsExtension> metricsExtension;

  private final KsqlEngine ksqlEngine;
  private final Metrics metrics;
  private final MetricCollectors metricCollectors;

  public KsqlEngineMetrics(
      final String metricGroupPrefix,
      final KsqlEngine ksqlEngine,
      final Map<String, String> customMetricsTags,
      final Optional<KsqlMetricsExtension> metricsExtension,
      final MetricCollectors metricCollectors
  ) {
    this(
        metricGroupPrefix.isEmpty() ? DEFAULT_METRIC_GROUP_PREFIX : metricGroupPrefix,
        ksqlEngine,
        metricCollectors,
        customMetricsTags,
        metricsExtension);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public KsqlEngineMetrics(
      final String metricGroupPrefix,
      final KsqlEngine ksqlEngine,
      final MetricCollectors metricCollectors,
      final Map<String, String> customMetricsTags,
      final Optional<KsqlMetricsExtension> metricsExtension
  ) {
    this.ksqlEngine = ksqlEngine;
    this.ksqlServiceIdLegacyPrefix = ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX
        + ksqlEngine.getServiceId();
    this.ksqlServicePrefix = ReservedInternalTopics.CONFLUENT_PREFIX;
    this.sensors = new ArrayList<>();
    this.countMetrics = new ArrayList<>();
    this.metricGroupPrefix = Objects.requireNonNull(metricGroupPrefix, "metricGroupPrefix");
    this.metricGroupName = metricGroupPrefix + METRIC_GROUP_POST_FIX;
    this.customMetricsTags = customMetricsTags;

    this.newCustomMetricsTags = ImmutableMap.<String, String>builder()
        .putAll(customMetricsTags)
        .put(KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, ksqlEngine.getServiceId())
        .build();
    this.metricsExtension = metricsExtension;

    this.metrics = metricCollectors.getMetrics();
    this.metricCollectors = metricCollectors;

    configureLivenessIndicator();
    configureNumActiveQueries();
    configureNumPersistentQueries();
    this.messagesIn = configureMessagesIn();
    this.totalMessagesIn = configureTotalMessagesIn();
    this.totalBytesIn = configureTotalBytesIn();
    this.messagesOut = configureMessagesOut();
    this.numIdleQueries = configureIdleQueriesSensor();
    this.messageConsumptionByQuery = configureMessageConsumptionByQuerySensor();
    this.errorRate = configureErrorRate();
    Arrays.stream(State.values())
        .forEach(this::configureNumActiveQueriesForGivenState);

    configureCustomMetrics();
  }

  @Override
  public void close() {
    sensors.forEach(sensor -> metrics.removeSensor(sensor.name()));
    countMetrics.forEach(countMetric -> metrics.removeMetric(countMetric.getMetricName()));
  }

  public void updateMetrics() {
    recordMessagesConsumed(metricCollectors.currentConsumptionRate());
    recordTotalMessagesConsumed(metricCollectors.totalMessageConsumption());
    recordTotalBytesConsumed(metricCollectors.totalBytesConsumption());
    recordMessagesProduced(metricCollectors.currentProductionRate());
    recordMessageConsumptionByQueryStats(metricCollectors.currentConsumptionRateByQuery());
    recordErrorRate(metricCollectors.currentErrorRate());
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "should be mutable")
  public Metrics getMetrics() {
    return metrics;
  }

  // Visible for testing
  List<Sensor> registeredSensors() {
    return sensors;
  }

  public QueryEventListener getQueryEventListener() {
    final String metricsPrefix
        = metricGroupPrefix.equals(KsqlEngineMetrics.DEFAULT_METRIC_GROUP_PREFIX)
        ? "" : metricGroupPrefix;
    return new QueryStateMetricsReportingListener(metrics, metricsPrefix, newCustomMetricsTags);
  }

  private void recordMessageConsumptionByQueryStats(
      final Collection<Double> messagesConsumedByQuery) {
    numIdleQueries.record(messagesConsumedByQuery.stream().filter(value -> value == 0.0).count());
    messagesConsumedByQuery.forEach(this.messageConsumptionByQuery::record);
  }

  private void recordMessagesProduced(final double value) {
    this.messagesOut.record(value);
  }

  private void recordMessagesConsumed(final double value) {
    this.messagesIn.record(value);
  }

  private void recordTotalBytesConsumed(final double value) {
    this.totalBytesIn.record(value);
  }

  private void recordTotalMessagesConsumed(final double value) {
    this.totalMessagesIn.record(value);
  }

  private void recordErrorRate(final double value) {
    this.errorRate.record(value);
  }

  private Sensor configureErrorRate() {
    final String metricName = "error-rate";
    final String description =
        "The number of messages which were consumed but not processed. "
        + "Messages may not be processed if, for instance, the message "
        + "contents could not be deserialized due to an incompatible schema. "
        + "Alternately, a consumed messages may not have been produced, hence "
        + "being effectively dropped. Such messages would also be counted "
        + "toward the error rate.";
    return createSensor(KsqlMetric.of(metricName, description, Value::new));
  }

  private Sensor configureMessagesOut() {
    final String metricName = "messages-produced-per-sec";
    final String description = "The number of messages produced per second across all queries";
    return createSensor(KsqlMetric.of(metricName, description, Value::new));
  }

  private Sensor configureMessagesIn() {
    final String metricName = "messages-consumed-per-sec";
    final String description = "The number of messages consumed per second across all queries";
    return createSensor(KsqlMetric.of(metricName, description, Value::new));
  }

  private Sensor configureTotalMessagesIn() {
    final String metricName = "messages-consumed-total";
    final String description = "The total number of messages consumed across all queries";
    return createSensor(KsqlMetric.of(metricName, description, Value::new));
  }

  private Sensor configureTotalBytesIn() {
    final String metricName = "bytes-consumed-total";
    final String description = "The total number of bytes consumed across all queries";
    return createSensor(KsqlMetric.of(metricName, description, Value::new));
  }

  private void configureNumActiveQueries() {
    final String metricName = "num-active-queries";
    final String description = "The current number of active queries running in this engine";
    final Supplier<MeasurableStat> statSupplier =
        () -> new MeasurableStat() {
          @Override
          public double measure(final MetricConfig metricConfig, final long l) {
            return ksqlEngine.numberOfLiveQueries();
          }

          @Override
          public void record(final MetricConfig metricConfig, final double v, final long l) {
            // We don't want to record anything, since the engine tracks query counts internally
          }
        };
    createSensor(KsqlMetric.of(metricName, description, statSupplier));
  }

  private void configureNumPersistentQueries() {
    final String metricName = "num-persistent-queries";
    final String description = "The current number of persistent queries running in this engine";
    final Supplier<MeasurableStat> statSupplier =
        () -> new MeasurableStat() {
          @Override
          public double measure(final MetricConfig metricConfig, final long l) {
            return ksqlEngine.getPersistentQueries().size();
          }

          @Override
          public void record(final MetricConfig metricConfig, final double v, final long l) {
            // We don't want to record anything, since the engine tracks query counts internally
          }
        };
    createSensor(KsqlMetric.of(metricName, description, statSupplier));
  }

  private Sensor configureIdleQueriesSensor() {
    final String metricName = "num-idle-queries";
    final String description = "Number of inactive queries";
    return createSensor(KsqlMetric.of(metricName, description, Value::new));
  }

  private void configureLivenessIndicator() {
    final String metricName = "liveness-indicator";
    final String description =
        "A metric with constant value 1 indicating the server is up and emitting metrics";
    final Supplier<MeasurableStat> statSupplier =
        () -> new MeasurableStat() {
          @Override
          public double measure(final MetricConfig metricConfig, final long l) {
            return 1;
          }

          @Override
          public void record(final MetricConfig metricConfig, final double v, final long l) {
            // Nothing to record
          }
        };
    createSensor(KsqlMetric.of(metricName, description, statSupplier));
  }

  private Sensor configureMessageConsumptionByQuerySensor() {
    final Sensor sensor = createSensor("message-consumption-by-query");
    configureMetric(
        sensor,
        KsqlMetric.of("messages-consumed-max", "max msgs consumed by query", Max::new)
    );
    configureMetric(
        sensor,
        KsqlMetric.of("messages-consumed-min", "min msgs consumed by query", Min::new)
    );
    configureMetric(
        sensor,
        KsqlMetric.of("messages-consumed-avg", "mean msgs consumed by query", Avg::new)
    );
    return sensor;
  }

  private void configureMetric(
      final Sensor sensor,
      final KsqlMetric metric) {
    // legacy
    sensor.add(
        metrics.metricName(
            metric.name(),
            ksqlServiceIdLegacyPrefix + metricGroupName,
            metric.description(),
            customMetricsTags),
        metric.statSupplier().get());
    // new metrics with service id in tag
    sensor.add(
        metrics.metricName(
            metric.name(),
            ksqlServicePrefix + metricGroupName,
            metric.description(),
            newCustomMetricsTags),
        metric.statSupplier().get());
  }

  private Sensor createSensor(final String sensorName) {
    final Sensor sensor = metrics.sensor(metricGroupName + "-" + sensorName);
    sensors.add(sensor);
    return sensor;
  }

  private Sensor createSensor(final KsqlMetric metric) {
    final Sensor sensor = createSensor(metric.name());
    configureMetric(sensor, metric);
    return sensor;
  }

  private void configureGaugeForState(
      final String name,
      final String group,
      final Map<String, String> tags,
      final KafkaStreams.State state
  ) {
    final Gauge<Long> gauge =
        (metricConfig, l) ->
            ksqlEngine.getPersistentQueries()
                .stream()
                .filter(queryMetadata -> queryMetadata.getState().equals(state))
                .count();
    final String description = String.format("Count of queries in %s state.", state.toString());
    final MetricName metricName = metrics.metricName(name, group, description, tags);
    final CountMetric countMetric = new CountMetric(metricName, gauge);
    try {
      metrics.addMetric(metricName, gauge);
    } catch (IllegalArgumentException e) {
      //not duplicate metrics, can be improved
    }
    countMetrics.add(countMetric);
  }

  private void configureNumActiveQueriesForGivenState(
      final KafkaStreams.State state) {
    final String name = state + "-queries";
    // legacy
    configureGaugeForState(
        name,
        ksqlServiceIdLegacyPrefix + metricGroupName,
        customMetricsTags,
        state
    );
    // new metrics with service id in tag
    configureGaugeForState(
        name,
        ksqlServicePrefix + metricGroupName,
        newCustomMetricsTags,
        state
    );
  }

  private void configureCustomMetrics() {
    if (!metricsExtension.isPresent()) {
      return;
    }

    final List<KsqlMetric> customMetrics = metricsExtension.get().getCustomMetrics();
    customMetrics.forEach(this::createSensor);
  }

  private static class CountMetric {
    private final Gauge<Long> count;
    private final MetricName metricName;

    CountMetric(final MetricName metricName, final Gauge<Long> count) {
      Objects.requireNonNull(metricName, "Metric name cannot be null.");
      Objects.requireNonNull(count, "Count gauge cannot be null.");
      this.metricName = metricName;
      this.count = count;
    }

    MetricName getMetricName() {
      return metricName;
    }

    public Gauge<Long> getCount() {
      return count;
    }
  }
}