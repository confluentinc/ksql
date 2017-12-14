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
import org.apache.kafka.common.metrics.stats.Value;

import java.io.Closeable;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metrics.MetricCollectors;

public class KsqlEngineMetrics implements Closeable {
  private final String metricGroupName;
  private final Sensor numActiveQueries;
  private final Sensor messagesIn;
  private final Sensor messagesOut;

  private final KsqlEngine ksqlEngine;

  public KsqlEngineMetrics(String metricGroupPrefix, KsqlEngine ksqlEngine) {
    Metrics metrics = MetricCollectors.getMetrics();

    this.ksqlEngine = ksqlEngine;

    this.metricGroupName = metricGroupPrefix + "-query-stats";
    this.numActiveQueries = metrics.sensor(metricGroupName + "-active-queries");
    numActiveQueries.add(
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

    numActiveQueries.add(
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

    this.messagesIn = metrics.sensor(metricGroupName + "-messages-consumed");
    this.messagesIn.add(
        metrics.metricName("messages-consumed-per-sec", this.metricGroupName,
                           "The number of messages consumed per second across all queries"),
        new Value());


    this.messagesOut = metrics.sensor(metricGroupName + "-messages-produced");
    this.messagesOut.add(
        metrics.metricName("messages-produced-per-sec", this.metricGroupName,
                           "The number of messages produced per second across all queries"),
        new Value());
  }

  public void recordMessagesProduced(double value) {
    this.messagesOut.record(value);
  }

  public void recordMessagesConsumed(double value) {
    this.messagesIn.record(value);
  }

  @Override
  public void close() {
    Metrics metrics = MetricCollectors.getMetrics();
    metrics.removeSensor(numActiveQueries.name());
    metrics.removeSensor(messagesIn.name());
    metrics.removeSensor(messagesOut.name());
  }
}