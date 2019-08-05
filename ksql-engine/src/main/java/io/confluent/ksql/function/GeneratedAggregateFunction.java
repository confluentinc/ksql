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

@SuppressWarnings({"unused", "WeakerAccess"}) // used in generated code
public abstract class GeneratedAggregateFunction<V, A> extends BaseAggregateFunction<V, A> {

  protected final Sensor aggregateSensor;
  protected final Sensor mergeSensor;
  protected Udaf<V, A> udaf;

  public GeneratedAggregateFunction(
      final String functionName,
      final Schema returnType,
      final List<Schema> arguments,
      final String description,
      final Optional<Metrics> metrics) {
    super(functionName, -1, null, returnType, arguments, description);

    final String method = getSourceMethodName();
    final String aggSensorName = String.format("aggregate-%s-%s", functionName, method);
    final String mergeSensorName = String.format("merge-%s-%s", functionName, method);

    initMetrics(metrics, functionName, method, aggSensorName, mergeSensorName);
    this.aggregateSensor = metrics.map(m -> m.getSensor(aggSensorName)).orElse(null);
    this.mergeSensor = metrics.map(m -> m.getSensor(mergeSensorName)).orElse(null);
  }

  protected GeneratedAggregateFunction(
      final String functionName,
      final int udafIndex,
      final Supplier<A> udafSupplier,
      final Schema returnType,
      final List<Schema> arguments,
      final String description,
      final Sensor aggregateSensor,
      final Sensor mergeSensor) {
    super(functionName, udafIndex, udafSupplier, returnType, arguments, description);
    this.aggregateSensor = aggregateSensor;
    this.mergeSensor = mergeSensor;
  }

  protected abstract String getSourceMethodName();

  protected Udaf<V, A> getUdaf() {
    return udaf;
  }

  private void initMetrics(
      final Optional<Metrics> maybeMetrics,
      final String name,
      final String method,
      final String aggSensorName,
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
      }
    }
  }

  @Override
  public A aggregate(final V currentValue, final A aggregateValue) {
    final long start = Time.SYSTEM.nanoseconds();
    try {
      //noinspection unchecked
      return udaf.aggregate(currentValue,aggregateValue);
    } finally {
      if (aggregateSensor != null) {
        aggregateSensor.record(Time.SYSTEM.nanoseconds() - start);
      }
    }
  }

  @Override
  public Merger<Struct, A> getMerger() {
    return (key, v1, v2) -> {
      final long start = Time.SYSTEM.nanoseconds();
      try {
        //noinspection unchecked
        return udaf.merge(v1, v2);
      } finally {
        if (mergeSensor != null) {
          mergeSensor.record(Time.SYSTEM.nanoseconds() - start);
        }
      }
    };
  }

  protected static <V,A> Supplier<A> supplier(final Udaf<V, A> udaf) {
    return udaf::initialize;
  }

}
