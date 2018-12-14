/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.internal;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.QueryMetadata;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
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

  private final List<Sensor> sensors;
  private final List<CountMetric> countMetrics;
  private final String metricGroupName;
  private final Sensor messagesIn;
  private final Sensor totalMessagesIn;
  private final Sensor totalBytesIn;
  private final Sensor messagesOut;
  private final Sensor numIdleQueries;
  private final Sensor messageConsumptionByQuery;
  private final Sensor errorRate;

  private final String ksqlServiceId;


  private final KsqlEngine ksqlEngine;
  private final Metrics metrics;

  public KsqlEngineMetrics(final String metricGroupPrefix, final KsqlEngine ksqlEngine) {
    this(metricGroupPrefix, ksqlEngine, MetricCollectors.getMetrics());
  }

  KsqlEngineMetrics(final String metricGroupPrefix, final KsqlEngine ksqlEngine,
      final Metrics metrics) {
    this.ksqlEngine = ksqlEngine;
    this.ksqlServiceId = KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX + ksqlEngine.getServiceId();
    this.sensors = new ArrayList<>();
    this.countMetrics = new ArrayList<>();
    this.metricGroupName = metricGroupPrefix + "-query-stats";

    this.metrics = metrics;

    configureNumActiveQueries(metrics);
    this.messagesIn = configureMessagesIn(metrics);
    this.totalMessagesIn = configureTotalMessagesIn(metrics);
    this.totalBytesIn = configureTotalBytesIn(metrics);
    this.messagesOut = configureMessagesOut(metrics);
    this.numIdleQueries = configureIdleQueriesSensor(metrics);
    this.messageConsumptionByQuery = configureMessageConsumptionByQuerySensor(metrics);
    this.errorRate = configureErrorRate(metrics);
    Arrays.stream(State.values())
        .forEach(state -> configureNumActiveQueriesForGivenState(metrics, state));
  }

  @Override
  public void close() {
    sensors.forEach(sensor -> metrics.removeSensor(sensor.name()));
    countMetrics.forEach(countMetric -> metrics.removeMetric(countMetric.getMetricName()));
  }

  public void updateMetrics() {
    recordMessagesConsumed(MetricCollectors.currentConsumptionRate());
    recordTotalMessagesConsumed(MetricCollectors.totalMessageConsumption());
    recordTotalBytesConsumed(MetricCollectors.totalBytesConsumption());
    recordMessagesProduced(MetricCollectors.currentProductionRate());
    recordMessageConsumptionByQueryStats(MetricCollectors.currentConsumptionRateByQuery());
    recordErrorRate(MetricCollectors.currentErrorRate());
  }

  public Metrics getMetrics() {
    return metrics;
  }

  // Visible for testing
  List<Sensor> registeredSensors() {
    return sensors;
  }

  public void registerQueries(final List<QueryMetadata> queryMetadataList) {
    queryMetadataList.forEach(queryMetadata -> queryMetadata.registerQueryStateListener(
        new QueryStateListener(
            metrics,
            queryMetadata.getKafkaStreams(),
            queryMetadata.getQueryApplicationId()
        )
    ));
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

  private Sensor configureErrorRate(final Metrics metrics) {
    final Sensor sensor = createSensor(metrics, metricGroupName + "-error-rate");
    sensor.add(
        metrics.metricName(ksqlServiceId + "error-rate", this.metricGroupName,
            "The number of messages which were consumed but not processed. "
                + "Messages may not be processed if, for instance, the message "
                + "contents could not be deserialized due to an incompatible schema. "
                + "Alternately, a consumed messages may not have been produced, hence "
                + "being effectively dropped. Such messages would also be counted "
                + "toward the error rate."),
        new Value());
    return sensor;
  }

  private Sensor configureMessagesOut(final Metrics metrics) {
    final Sensor sensor = createSensor(metrics, metricGroupName + "-messages-produced");
    sensor.add(
        metrics.metricName(ksqlServiceId + "messages-produced-per-sec", this.metricGroupName,
            "The number of messages produced per second across all queries"),
        new Value());

    return sensor;
  }

  private Sensor configureMessagesIn(final Metrics metrics) {
    final Sensor sensor = createSensor(metrics, metricGroupName + "-messages-consumed");
    sensor.add(
        metrics.metricName(ksqlServiceId + "messages-consumed-per-sec", this.metricGroupName,
            "The number of messages consumed per second across all queries"),
        new Value());
    return sensor;
  }

  private Sensor configureTotalMessagesIn(final Metrics metrics) {
    final Sensor sensor = createSensor(metrics, metricGroupName + "-total-messages-consumed");
    sensor.add(
        metrics.metricName(ksqlServiceId + "messages-consumed-total", this.metricGroupName,
            "The total number of messages consumed across all queries"),
        new Value());
    return sensor;
  }

  private Sensor configureTotalBytesIn(final Metrics metrics) {
    final Sensor sensor = createSensor(metrics, metricGroupName + "-total-bytes-consumed");
    sensor.add(
        metrics.metricName(ksqlServiceId + "bytes-consumed-total", this.metricGroupName,
            "The total number of bytes consumed across all queries"),
        new Value());
    return sensor;
  }

  private void configureNumActiveQueries(final Metrics metrics) {
    final Sensor sensor = createSensor(metrics, metricGroupName + "-active-queries");
    sensor.add(
        metrics.metricName(ksqlServiceId + "num-active-queries", this.metricGroupName,
            "The current number of active queries running in this engine"),
        new MeasurableStat() {
          @Override
          public double measure(final MetricConfig metricConfig, final long l) {
            return ksqlEngine.numberOfLiveQueries();
          }

          @Override
          public void record(final MetricConfig metricConfig, final double v, final long l) {
            // We don't want to record anything, since the live queries anyway.
          }
        });

    sensor.add(
        metrics.metricName(ksqlServiceId + "num-persistent-queries", this.metricGroupName,
            "The current number of persistent queries running in this engine"),
        new MeasurableStat() {
          @Override
          public double measure(final MetricConfig metricConfig, final long l) {
            return ksqlEngine.numberOfPersistentQueries();
          }

          @Override
          public void record(final MetricConfig metricConfig, final double v, final long l) {
            // No action for record since we can read the desired results directly.
          }
        }
    );
  }

  private Sensor configureIdleQueriesSensor(final Metrics metrics) {
    final Sensor sensor = createSensor(metrics, "num-idle-queries");
    sensor.add(metrics.metricName(
        ksqlServiceId + "num-idle-queries", this.metricGroupName),
        new Value());
    return sensor;
  }

  private Sensor configureMessageConsumptionByQuerySensor(final Metrics metrics) {
    final Sensor sensor = createSensor(
        metrics,
        ksqlServiceId + "message-consumption-by-query");
    sensor.add(
        metrics.metricName(ksqlServiceId + "messages-consumed-max", this.metricGroupName),
        new Max());
    sensor.add(
        metrics.metricName(ksqlServiceId + "messages-consumed-min", this.metricGroupName),
        new Min());
    sensor.add(
        metrics.metricName(ksqlServiceId + "messages-consumed-avg", this.metricGroupName),
        new Avg());
    return sensor;
  }

  private Sensor createSensor(final Metrics metrics, final String sensorName) {
    final Sensor sensor = metrics.sensor(sensorName);
    sensors.add(sensor);
    return sensor;
  }


  private CountMetric configureNumActiveQueriesForGivenState(final Metrics metrics,
      final KafkaStreams.State state) {
    final String gaugeName = ksqlServiceId + metricGroupName + "-" + state + "-queries";
    final Gauge<Long> gauge = (metricConfig, l) -> ksqlEngine.getPersistentQueries()
        .stream()
        .filter(queryMetadata -> queryMetadata.getKafkaStreams().state() == state)
        .count();

    final MetricName metricName = metrics.metricName(gaugeName,
        metricGroupName,
        String.format("Count of queries in %s state.", state.toString()));
    final CountMetric countMetric = new CountMetric(metricName, gauge);
    metrics.addMetric(metricName, gauge);
    countMetrics.add(countMetric);
    return countMetric;
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