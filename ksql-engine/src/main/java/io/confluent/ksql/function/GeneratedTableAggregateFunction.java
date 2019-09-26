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

import io.confluent.ksql.execution.function.TableAggregationFunction;
import io.confluent.ksql.function.udaf.TableUdaf;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.connect.data.Schema;

@SuppressWarnings("unused") // used in generated code
public abstract class GeneratedTableAggregateFunction<I, A, O>
    extends GeneratedAggregateFunction<I, A, O> implements TableAggregationFunction<I, A, O> {

  public GeneratedTableAggregateFunction(
      final String functionName,
      final Schema aggregateType,
      final Schema outputType,
      final List<Schema> arguments,
      final String description,
      final Optional<Metrics> metrics) {
    super(functionName, aggregateType, outputType, arguments, description, metrics);
  }

  protected GeneratedTableAggregateFunction(
      final String functionName,
      final int udafIndex,
      final Supplier<A> udafSupplier,
      final Schema aggregateType,
      final Schema outputType,
      final List<Schema> arguments,
      final String description,
      final Optional<Sensor> aggregateSensor,
      final Optional<Sensor> mapSensor,
      final Optional<Sensor> mergeSensor) {
    super(functionName, udafIndex, udafSupplier, aggregateType, outputType,
          arguments, description, aggregateSensor, mapSensor, mergeSensor);
  }

  @Override
  public A undo(final I valueToUndo, final A aggregateValue) {
    return ((TableUdaf<I, A, O>) getUdaf()).undo(valueToUndo, aggregateValue);
  }
}
