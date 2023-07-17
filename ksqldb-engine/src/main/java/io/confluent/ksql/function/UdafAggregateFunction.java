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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.security.ExtensionSecurityManager;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.kstream.Merger;

public class UdafAggregateFunction<I, A, O> extends BaseAggregateFunction<I, A, O> {

  protected final Optional<Sensor> aggregateSensor;
  protected final Optional<Sensor> mapSensor;
  protected final Optional<Sensor> mergeSensor;
  protected final Udaf<I, A, O> udaf;

  protected UdafAggregateFunction(
      final String functionName,
      final List<Integer> udafIndices,
      final Udaf<I, A, O> udaf,
      final SqlType aggregateType,
      final SqlType outputType,
      final List<ParameterInfo> arguments,
      final String description,
      final Optional<Metrics> metrics,
      final String method,
      final int numColArgs
  ) {
    super(functionName, udafIndices, udaf::initialize, aggregateType,
        outputType, arguments, description, numColArgs);

    this.udaf = Objects.requireNonNull(udaf, "udaf");

    final String groupName = String.format("ksql-udaf-%s-%s", functionName, method);

    this.aggregateSensor = getSensor(metrics, functionName, method, groupName, "aggregate");
    this.mapSensor = getSensor(metrics, functionName, method, groupName, "map");
    this.mergeSensor = getSensor(metrics, functionName, method, groupName, "merge");
  }

  @Override
  public A aggregate(final I currentValue, final A aggregateValue) {
    return timed(aggregateSensor, () -> udaf.aggregate(currentValue, aggregateValue));
  }

  @Override
  public Merger<GenericKey, A> getMerger() {
    return (key, v1, v2) -> timed(mergeSensor, () -> udaf.merge(v1, v2));
  }

  @Override
  public Function<A, O> getResultMapper() {
    return (v1) -> timed(mapSensor, () -> udaf.map(v1));
  }

  private static Optional<Sensor> getSensor(
      final Optional<Metrics> maybeMetrics,
      final String name,
      final String method,
      final String groupName,
      final String step
  ) {
    if (!maybeMetrics.isPresent()) {
      return Optional.empty();
    }

    final Metrics metrics = maybeMetrics.get();

    final String sensorName = step + "-" + name + "-" + method;

    final Sensor existing = metrics.getSensor(sensorName);
    if (existing != null) {
      return Optional.of(existing);
    }

    final Sensor newSensor = FunctionMetrics.getInvocationSensor(
        metrics,
        sensorName,
        groupName,
        name + " " + method + " udaf's " + step + " step"
    );

    return Optional.of(newSensor);
  }

  private static <T> T timed(final Optional<Sensor> maybeSensor, final Supplier<T> task) {
    final long start = Time.SYSTEM.nanoseconds();
    try {
      // Since the timed() function wraps the calls to Udafs, we use it to protect the calls.
      ExtensionSecurityManager.INSTANCE.pushInUdf();
      return task.get();
    } finally {
      maybeSensor.ifPresent(sensor -> sensor.record(Time.SYSTEM.nanoseconds() - start));
      ExtensionSecurityManager.INSTANCE.popOutUdf();
    }
  }
}
