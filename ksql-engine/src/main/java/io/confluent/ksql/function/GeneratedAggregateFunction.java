/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.function;

import io.confluent.ksql.function.udaf.Udaf;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

@SuppressWarnings({"unused", "unchecked", "WeakerAccess"}) // used in generated code
public abstract class GeneratedAggregateFunction extends BaseAggregateFunction {

  private final Time time = Time.SYSTEM;

  protected Udaf udaf;
  protected Sensor aggregateSensor;
  protected Sensor mergeSensor;

  public GeneratedAggregateFunction(final String functionName,
                                    final Schema returnType,
                                    final List<Schema> arguments,
                                    final String description) {
    super(functionName, -1, null, returnType, arguments, description);
  }

  protected GeneratedAggregateFunction(final String functionName,
                                       final int udafIndex,
                                       final Supplier udafSupplier,
                                       final Schema returnType,
                                       final List<Schema> arguments,
                                       final String description,
                                       final Sensor aggregateSensor,
                                       final Sensor mergeSensor) {
    super(functionName, udafIndex, udafSupplier, returnType, arguments, description);
    this.aggregateSensor = aggregateSensor;
    this.mergeSensor = mergeSensor;
  }

  protected Udaf getUdaf() {
    return udaf;
  }

  protected void initMetrics(
      final Optional<Metrics> maybeMetrics,
      final String name,
      final String method) {
    if (maybeMetrics.isPresent()) {
      final String groupName = String.format("ksql-udaf-%s-%s", name, method);
      final Metrics metrics = maybeMetrics.get();

      final String aggSensorName = String.format("aggregate-%s-%s", name, method);
      if (metrics.getSensor(aggSensorName) == null) {
        final Sensor sensor = metrics.sensor(aggSensorName);
        sensor.add(metrics.metricName(
            aggSensorName + "-avg",
            groupName,
            "Average time for an aggregate invocation of #NAME #METHOD udaf"),
                   new Avg());
        sensor.add(metrics.metricName(
            aggSensorName + "-max",
            groupName,
            "Max time for an aggregate invocation of #NAME #METHOD udaf"),
                   new Max());
        sensor.add(metrics.metricName(
            aggSensorName + "-count",
            groupName,
            "Total number of aggregate invocations of #NAME #METHOD udaf"),
                   new Count());
        sensor.add(metrics.metricName(
            aggSensorName + "-rate",
            groupName,
            "The average number of occurrences of aggregate "
                + "#NAME #METHOD operation per second udaf"),
                   new Rate(TimeUnit.SECONDS, new Count()));
      }

      final String mergeSensorName = "merge-#NAME-#METHOD";
      if (metrics.getSensor(mergeSensorName) == null) {
        final Sensor sensor = metrics.sensor(mergeSensorName);
        sensor.add(metrics.metricName(
            mergeSensorName + "-avg",
            groupName,
            "Average time for a merge invocation of #NAME #METHOD udaf"),
                   new Avg());
        sensor.add(metrics.metricName(
            mergeSensorName + "-max",
            groupName,
            "Max time for a merge invocation of #NAME #METHOD udaf"),
                   new Max());
        sensor.add(metrics.metricName(
            mergeSensorName + "-count",
            groupName,
            "Total number of merge invocations of #NAME #METHOD udaf"),
                   new Count());
        sensor.add(metrics.metricName(
            mergeSensorName + "-rate",
            groupName,
            "The average number of occurrences of merge #NAME #METHOD operation per second udaf"),
                   new Rate(TimeUnit.SECONDS, new Count()));
      }

      this.aggregateSensor = metrics.getSensor(aggSensorName);
      this.mergeSensor = metrics.getSensor(mergeSensorName);
    }
  }

  @Override
  public Object aggregate(final Object currentValue, final Object aggregateValue) {
    final long start = time.nanoseconds();
    try {
      return udaf.aggregate(currentValue,aggregateValue);
    } finally {
      if (aggregateSensor != null) {
        aggregateSensor.record(time.nanoseconds() - start);
      }
    }
  }

  @Override
  public Merger getMerger() {
    return (key, v1, v2) -> {
      final long start = time.nanoseconds();
      try {
        return udaf.merge(v1, v2);
      } finally {
        if (mergeSensor != null) {
          mergeSensor.record(time.nanoseconds() - start);
        }
      }
    };
  }

  protected static Supplier supplier(final Udaf udaf) {
    return udaf::initialize;
  }

}
