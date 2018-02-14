/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.internal;

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Value;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metrics.MetricCollectors;

public class KsqlEngineMetrics implements Closeable {
  private final List<Sensor> sensors;
  private final String metricGroupName;
  private final Sensor numActiveQueries;
  private final Sensor messagesIn;
  private final Sensor totalMessagesIn;
  private final Sensor totalBytesIn;
  private final Sensor messagesOut;
  private final Sensor numIdleQueries;
  private final Sensor messageConsumptionByQuery;
  private final Sensor errorRate;


  private final KsqlEngine ksqlEngine;

  public KsqlEngineMetrics(String metricGroupPrefix, KsqlEngine ksqlEngine) {
    this.ksqlEngine = ksqlEngine;
    this.sensors = new ArrayList<>();
    this.metricGroupName = metricGroupPrefix + "-query-stats";

    Metrics metrics = MetricCollectors.getMetrics();

    this.numActiveQueries = configureNumActiveQueries(metrics);
    this.messagesIn = configureMessagesIn(metrics);
    this.totalMessagesIn = configureTotalMessagesIn(metrics);
    this.totalBytesIn = configureTotalBytesIn(metrics);
    this.messagesOut =  configureMessagesOut(metrics);
    this.numIdleQueries = configureIdleQueriesSensor(metrics);
    this.messageConsumptionByQuery = configureMessageConsumptionByQuerySensor(metrics);
    this.errorRate = configureErrorRate(metrics);
  }

  @Override
  public void close() {
    Metrics metrics = MetricCollectors.getMetrics();
    sensors.forEach(sensor -> metrics.removeSensor(sensor.name()));
  }

  public void updateMetrics() {
    recordMessagesConsumed(MetricCollectors.currentConsumptionRate());
    recordTotalMessagesConsumed(MetricCollectors.totalMessageConsumption());
    recordTotalBytesConsumed(MetricCollectors.totalBytesConsumption());
    recordMessagesProduced(MetricCollectors.currentProductionRate());
    recordMessageConsumptionByQueryStats(MetricCollectors.currentConsumptionRateByQuery());
    recordErrorRate(MetricCollectors.currentErrorRate());
  }

  // Visible for testing
  List<Sensor> registeredSensors() {
    return sensors;
  }

  private void recordMessageConsumptionByQueryStats(Collection<Double> messagesConsumedByQuery) {
    numIdleQueries.record(messagesConsumedByQuery.stream().filter(value -> value == 0.0).count());
    messagesConsumedByQuery.forEach(this.messageConsumptionByQuery::record);
  }

  private void recordMessagesProduced(double value) {
    this.messagesOut.record(value);
  }

  private void recordMessagesConsumed(double value) {
    this.messagesIn.record(value);
  }

  private void recordTotalBytesConsumed(double value) {
    this.totalBytesIn.record(value);
  }

  private void recordTotalMessagesConsumed(double value) {
    this.totalMessagesIn.record(value);
  }

  private void recordErrorRate(double value) {
    this.errorRate.record(value);
  }

  private Sensor configureErrorRate(Metrics metrics) {
    Sensor sensor = createSensor(metrics, metricGroupName + "-error-rate");
    sensor.add(
        metrics.metricName("error-rate", this.metricGroupName,
                           "The number of messages which were consumed but not processed. "
                           + "Messages may not be processed if, for instance, the message "
                           + "contents could not be deserialized due to an incompatible schema. "
                           + "Alternately, a consumed messages may not have been produced, hence "
                           + "being effectively dropped. Such messages would also be counted "
                           + "toward the error rate."),
        new Value());
    return sensor;
  }

  private Sensor configureMessagesOut(Metrics metrics) {
    Sensor sensor = createSensor(metrics, metricGroupName + "-messages-produced");
    sensor.add(
        metrics.metricName("messages-produced-per-sec", this.metricGroupName,
                           "The number of messages produced per second across all queries"),
        new Value());

    return sensor;
  }

  private Sensor configureMessagesIn(Metrics metrics) {
    Sensor sensor = createSensor(metrics, metricGroupName + "-messages-consumed");
    sensor.add(
        metrics.metricName("messages-consumed-per-sec", this.metricGroupName,
                           "The number of messages consumed per second across all queries"),
        new Value());
    return sensor;
  }

  private Sensor configureTotalMessagesIn(Metrics metrics) {
    Sensor sensor = createSensor(metrics, metricGroupName + "-total-messages-consumed");
    sensor.add(
        metrics.metricName("messages-consumed-total", this.metricGroupName,
            "The total number of messages consumed across all queries"),
        new Value());
    return sensor;
  }

  private Sensor configureTotalBytesIn(Metrics metrics) {
    Sensor sensor = createSensor(metrics, metricGroupName + "-total-bytes-consumed");
    sensor.add(
        metrics.metricName("bytes-consumed-total", this.metricGroupName,
            "The total number of bytes consumed across all queries"),
        new Value());
    return sensor;
  }

  private Sensor configureNumActiveQueries(Metrics metrics) {
    Sensor sensor = createSensor(metrics, metricGroupName + "-active-queries");
    sensor.add(
        metrics.metricName("num-active-queries", this.metricGroupName,
                           "The current number of active queries running in this engine"),
        new MeasurableStat() {
          @Override
          public double measure(MetricConfig metricConfig, long l) {
            return ksqlEngine.numberOfLiveQueries();
          }

          @Override
          public void record(MetricConfig metricConfig, double v, long l) {
            // We don't want to record anything, since the live queries anyway.
          }
        });

    sensor.add(
        metrics.metricName("num-persistent-queries", this.metricGroupName,
                           "The current number of persistent queries running in this engine"),
        new MeasurableStat() {
          @Override
          public double measure(MetricConfig metricConfig, long l) {
            return ksqlEngine.numberOfPersistentQueries();
          }

          @Override
          public void record(MetricConfig metricConfig, double v, long l) {
            // No action for record since we can read the desired results directly.
          }
        }
    );
    return sensor;

  }

  private Sensor configureIdleQueriesSensor(Metrics metrics) {
    Sensor sensor = createSensor(metrics, "num-idle-queries");
    sensor.add(metrics.metricName("num-idle-queries", this.metricGroupName), new Value());
    return sensor;
  }

  private Sensor configureMessageConsumptionByQuerySensor(Metrics metrics) {
    Sensor sensor = createSensor(metrics, "message-consumption-by-query");
    sensor.add(metrics.metricName("messages-consumed-max", this.metricGroupName), new Max());
    sensor.add(metrics.metricName("messages-consumed-min", this.metricGroupName), new Min());
    sensor.add(metrics.metricName("messages-consumed-avg", this.metricGroupName), new Avg());
    return sensor;
  }

  private Sensor createSensor(Metrics metrics, String sensorName) {
    Sensor sensor = metrics.sensor(sensorName);
    sensors.add(sensor);
    return sensor;
  }

}