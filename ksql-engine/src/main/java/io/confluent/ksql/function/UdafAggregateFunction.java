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

package io.confluent.ksql.function;

import io.confluent.ksql.function.udaf.Udaf;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;

@SuppressWarnings("WeakerAccess")
public class UdafAggregateFunction<I, A, O> extends BaseAggregateFunction<I, A, O> {

  protected Optional<Sensor> aggregateSensor;
  protected Optional<Sensor> mapSensor;
  protected Optional<Sensor> mergeSensor;
  protected Udaf<I, A, O> udaf;

  protected UdafAggregateFunction(
      final String functionName,
      final int udafIndex,
      final Udaf<I, A, O> udaf,
      final Schema aggregateType,
      final Schema outputType,
      final List<Schema> arguments,
      final String description,
      final Optional<Metrics> metrics,
      final String method) {

    super(functionName, udafIndex, udaf::initialize, aggregateType,
        outputType, arguments, description);

    this.udaf = udaf;

    final String aggSensorName = String.format("aggregate-%s-%s", functionName, method);
    final String mapSensorName = String.format("map-%s-%s", functionName, method);
    final String mergeSensorName = String.format("merge-%s-%s", functionName, method);

    initMetrics(metrics, functionName, method, aggSensorName, mapSensorName, mergeSensorName);
  }

  private void initMetrics(
      final Optional<Metrics> maybeMetrics,
      final String name,
      final String method,
      final String aggSensorName,
      final String mapSensorName,
      final String mergeSensorName) {
    if (maybeMetrics.isPresent()) {
      final String groupName = String.format("ksql-udaf-%s-%s", name, method);
      final Metrics metrics = maybeMetrics.get();

      if (metrics.getSensor(aggSensorName) == null) {
        final Sensor sensor = metrics.sensor(aggSensorName);
        sensor.add(metrics.metricName(
            aggSensorName + "-avg",
            groupName,
            String.format("Average time for an aggregate invocation of %s %s udaf", name, method)),
            new Avg());
        sensor.add(metrics.metricName(
            aggSensorName + "-max",
            groupName,
            String.format("Max time for an aggregate invocation of %s %s udaf", name, method)),
            new Max());
        sensor.add(metrics.metricName(
            aggSensorName + "-count",
            groupName,
            String.format("Total number of aggregate invocations of %s %s udaf", name, method)),
            new WindowedCount());
        sensor.add(metrics.metricName(
            aggSensorName + "-rate",
            groupName,
            String.format("The average number of occurrences of aggregate "
                + "%s %s operation per second udaf", name, method)),
            new Rate(TimeUnit.SECONDS, new WindowedCount()));
        this.aggregateSensor = Optional.of(sensor);
      }

      if (metrics.getSensor(mapSensorName) == null) {
        final Sensor sensor = metrics.sensor(mapSensorName);
        sensor.add(metrics.metricName(
            mapSensorName + "-avg",
            groupName,
            String.format("Average time for a map invocation of %s %s udaf", name, method)),
            new Avg());
        sensor.add(metrics.metricName(
            mapSensorName + "-max",
            groupName,
            String.format("Max time for a map invocation of %s %s udaf", name, method)),
            new Max());
        sensor.add(metrics.metricName(
            mapSensorName + "-count",
            groupName,
            String.format("Total number of map invocations of %s %s udaf", name, method)),
            new WindowedCount());
        sensor.add(metrics.metricName(
            mapSensorName + "-rate",
            groupName,
            String.format("The average number of occurrences of map "
                + "%s %s operation per second udaf", name, method)),
            new Rate(TimeUnit.SECONDS, new WindowedCount()));
        this.mapSensor = Optional.of(sensor);
      }

      if (metrics.getSensor(mergeSensorName) == null) {
        final Sensor sensor = metrics.sensor(mergeSensorName);
        sensor.add(metrics.metricName(
            mergeSensorName + "-avg",
            groupName,
            String.format("Average time for a merge invocation of %s %s udaf", name, method)),
            new Avg());
        sensor.add(metrics.metricName(
            mergeSensorName + "-max",
            groupName,
            String.format("Max time for a merge invocation of %s %s udaf", name, method)),
            new Max());
        sensor.add(metrics.metricName(
            mergeSensorName + "-count",
            groupName,
            String.format("Total number of merge invocations of %s %s udaf", name, method)),
            new WindowedCount());
        sensor.add(metrics.metricName(
            mergeSensorName + "-rate",
            groupName,
            String.format(
                "The average number of occurrences of merge %s %s operation per second udaf",
                name, method)),
            new Rate(TimeUnit.SECONDS, new WindowedCount()));
        this.mergeSensor = Optional.of(sensor);
      }
    } else {
      this.aggregateSensor = Optional.empty();
      this.mapSensor = Optional.empty();
      this.mergeSensor = Optional.empty();
    }

  }

  @Override
  public A aggregate(final I currentValue, final A aggregateValue) {
    return timed(aggregateSensor, () -> udaf.aggregate(currentValue, aggregateValue));
  }

  @Override
  public Merger<Struct, A> getMerger() {
    return (key, v1, v2) -> timed(mergeSensor, () -> udaf.merge(v1, v2));
  }

  @Override
  public Function<A, O> getResultMapper() {
    return (v1) -> timed(mapSensor, () -> udaf.map(v1));
  }

  private static <T> T timed(final Optional<Sensor> maybeSensor, final Supplier<T> task) {
    final long start = Time.SYSTEM.nanoseconds();
    try {
      return task.get();
    } finally {
      maybeSensor.ifPresent(sensor -> sensor.record(Time.SYSTEM.nanoseconds() - start));
    }
  }
}
