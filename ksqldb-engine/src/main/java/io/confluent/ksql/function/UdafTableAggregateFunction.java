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
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;

public class UdafTableAggregateFunction<I, A, O>
    extends UdafAggregateFunction<I, A, O> implements TableAggregationFunction<I, A, O> {

  public UdafTableAggregateFunction(
      final String functionName,
      final int udafIndex,
      final Udaf<I, A, O> udaf,
      final SqlType aggregateType,
      final SqlType outputType,
      final List<ParameterInfo> parameters,
      final String description,
      final Optional<Metrics> metrics,
      final String method) {
    super(functionName, udafIndex, udaf, aggregateType, outputType, parameters, description,
        metrics, method);
  }

  @Override
  public A undo(final I valueToUndo, final A aggregateValue) {
    return ((TableUdaf<I, A, O>)udaf).undo(valueToUndo, aggregateValue);
  }
}
