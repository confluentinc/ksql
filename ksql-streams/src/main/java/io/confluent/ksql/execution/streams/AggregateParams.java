/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.function.TableAggregationFunction;
import io.confluent.ksql.execution.function.UdafUtil;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.function.udaf.KudafInitializer;
import io.confluent.ksql.execution.function.udaf.KudafUndoAggregator;
import io.confluent.ksql.execution.function.udaf.window.WindowSelectMapper;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class AggregateParams {
  private final KudafInitializer initializer;
  private final int initialUdafIndex;
  private final Map<Integer, KsqlAggregateFunction> indexToFunction;

  AggregateParams(
      final LogicalSchema internalSchema,
      final int initialUdafIndex,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList
  ) {
    final List<Supplier> initialValueSuppliers = new LinkedList<>();
    int udafIndexInAggSchema = initialUdafIndex;
    final Map<Integer, KsqlAggregateFunction> indexToFunction = new HashMap<>();
    for (final FunctionCall functionCall : functionList) {
      final KsqlAggregateFunction aggregateFunction = UdafUtil.resolveAggregateFunction(
          functionRegistry,
          functionCall,
          internalSchema
      );

      indexToFunction.put(udafIndexInAggSchema++, aggregateFunction);
      initialValueSuppliers.add(aggregateFunction.getInitialValueSupplier());
    }
    this.initialUdafIndex = initialUdafIndex;
    this.initializer = new KudafInitializer(initialUdafIndex, initialValueSuppliers);
    this.indexToFunction = ImmutableMap.copyOf(indexToFunction);
  }

  public KudafInitializer getInitializer() {
    return initializer;
  }

  public KudafAggregator getAggregator() {
    return new KudafAggregator(initialUdafIndex, indexToFunction);
  }

  public KudafUndoAggregator getUndoAggregator() {
    final Map<Integer, TableAggregationFunction> indexToUndo =
        indexToFunction.keySet()
            .stream()
            .collect(
                Collectors.toMap(
                    k -> k,
                    k -> ((TableAggregationFunction) indexToFunction.get(k))));
    return new KudafUndoAggregator(initialUdafIndex, indexToUndo);
  }

  public WindowSelectMapper getWindowSelectMapper() {
    return new WindowSelectMapper(indexToFunction);
  }

  public interface Factory {
    AggregateParams create(
        LogicalSchema internalSchema,
        int initialUdafIndex,
        FunctionRegistry functionRegistry,
        List<FunctionCall> functionList
    );
  }
}
