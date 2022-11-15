/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.function;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;

public final class FunctionMetrics {

  static final String AVG_DESC = "Average time for invocations of the %s";
  static final String MAX_DESC = "Max time for invocations of the %s";
  static final String COUNT_DESC = "Total number of invocations of the %s";
  static final String RATE_DESC = "The rate of invocations (invocations per second) of the %s";

  private FunctionMetrics() {
  }

  /**
   * Gets an existing invocation sensor, or creates one if needed.
   *
   * <p>Sensor created with avg, max, count and rate metrics.
   *
   * @param metrics the metrics service.
   * @param sensorName the name of the sensor
   * @param groupName the name of the group
   * @param functionDescription the description of the function.
   */
  public static void initInvocationSensor(
      final Optional<Metrics> metrics,
      final String sensorName,
      final String groupName,
      final String functionDescription
  ) {
    metrics.ifPresent(m -> getInvocationSensor(m, sensorName, groupName, functionDescription));
  }

  /**
   * Gets an existing invocation sensor, or creates one if needed.
   *
   * <p>Sensor created with avg, max, count and rate metrics.
   *
   * @param metrics the metrics service.
   * @param sensorName the name of the sensor
   * @param groupName the name of the group
   * @param functionDescription the description of the function.
   */
  public static Sensor getInvocationSensor(
      final Metrics metrics,
      final String sensorName,
      final String groupName,
      final String functionDescription
  ) {
    final Sensor sensor = metrics.sensor(sensorName);
    if (sensor.hasMetrics()) {
      return sensor;
    }

    final BiFunction<String, String, MetricName> metricNamer = (suffix, descPattern) -> {
      final String description = String.format(descPattern, functionDescription);
      return metrics.metricName(sensorName + "-" + suffix, groupName, description);
    };

    sensor.add(
        metricNamer.apply("avg", AVG_DESC),
        new Avg()
    );

    sensor.add(
        metricNamer.apply("max", MAX_DESC),
        new Max()
    );

    sensor.add(
        metricNamer.apply("count", COUNT_DESC),
        new WindowedCount()
    );

    sensor.add(
        metricNamer.apply("rate", RATE_DESC),
        new Rate(TimeUnit.SECONDS, new WindowedCount())
    );

    return sensor;
  }
}
