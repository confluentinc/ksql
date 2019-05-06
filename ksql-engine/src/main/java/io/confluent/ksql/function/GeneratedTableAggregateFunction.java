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

import io.confluent.ksql.function.udaf.TableUdaf;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.connect.data.Schema;

@SuppressWarnings("unused") // used in generated code
public abstract class GeneratedTableAggregateFunction<V, A>
    extends GeneratedAggregateFunction<V, A> implements TableAggregationFunction<V, A> {

  public GeneratedTableAggregateFunction(
      final String functionName,
      final Schema returnType,
      final List<Schema> arguments,
      final String description,
      final Optional<Metrics> metrics) {
    super(functionName, returnType, arguments, description, metrics);
  }

  protected GeneratedTableAggregateFunction(
      final String functionName,
      final int udafIndex,
      final Supplier<A> udafSupplier,
      final Schema returnType,
      final List<Schema> arguments,
      final String description,
      final Sensor aggregateSensor,
      final Sensor mergeSensor) {
    super(functionName, udafIndex, udafSupplier, returnType, arguments, description,
          aggregateSensor,
          mergeSensor);
  }

  @Override
  public A undo(final V valueToUndo, final A aggregateValue) {
    return ((TableUdaf<V, A>) getUdaf()).undo(valueToUndo, aggregateValue);
  }
}
