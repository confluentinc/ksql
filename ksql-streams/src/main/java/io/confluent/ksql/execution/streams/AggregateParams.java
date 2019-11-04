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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
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
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class AggregateParams {

  private final KudafInitializer initializer;
  private final int initialUdafIndex;
  private final List<KsqlAggregateFunction<?, ?, ?>> functions;
  private final KudafAggregatorFactory aggregatorFactory;

  public AggregateParams(
      final LogicalSchema internalSchema,
      final int initialUdafIndex,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList
  ) {
    this(internalSchema, initialUdafIndex, functionRegistry, functionList, KudafAggregator::new);
  }

  @VisibleForTesting
  AggregateParams(
      final LogicalSchema internalSchema,
      final int initialUdafIndex,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList,
      final KudafAggregatorFactory aggregatorFactory
  ) {
    this.initialUdafIndex = initialUdafIndex;
    this.functions = ImmutableList.copyOf(functionList.stream()
        .map(funcCall -> UdafUtil.resolveAggregateFunction(
            functionRegistry,
            funcCall,
            internalSchema
        )).collect(Collectors.toList()));

    final List<Supplier<?>> initialValueSuppliers = functions.stream()
        .map(KsqlAggregateFunction::getInitialValueSupplier)
        .collect(Collectors.toList());

    this.initializer = new KudafInitializer(initialUdafIndex, initialValueSuppliers);
    this.aggregatorFactory = requireNonNull(aggregatorFactory, "aggregatorFactory");
  }

  public KudafInitializer getInitializer() {
    return initializer;
  }

  public KudafAggregator getAggregator() {
    return aggregatorFactory.create(initialUdafIndex, functions);
  }

  public KudafUndoAggregator getUndoAggregator() {
    final List<TableAggregationFunction<?, ?, ?>> tableFunctions = new LinkedList<>();
    for (final KsqlAggregateFunction function : functions) {
      tableFunctions.add((TableAggregationFunction<?, ?, ?>) function);
    }
    return new KudafUndoAggregator(initialUdafIndex, tableFunctions);
  }

  public WindowSelectMapper getWindowSelectMapper() {
    return new WindowSelectMapper(initialUdafIndex, functions);
  }

  public interface Factory {

    AggregateParams create(
        LogicalSchema internalSchema,
        int initialUdafIndex,
        FunctionRegistry functionRegistry,
        List<FunctionCall> functionList
    );
  }

  interface KudafAggregatorFactory {

    KudafAggregator create(
        int initialUdafIndex,
        List<KsqlAggregateFunction<?, ?, ?>> functions
    );
  }
}
