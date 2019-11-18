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
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AggregateParamsFactory {
  private final KudafAggregatorFactory aggregatorFactory;

  public AggregateParamsFactory() {
    this(KudafAggregator::new);
  }

  @VisibleForTesting
  AggregateParamsFactory(final KudafAggregatorFactory aggregatorFactory) {
    this.aggregatorFactory = Objects.requireNonNull(aggregatorFactory);
  }

  public AggregateParams createUndoable(
      final LogicalSchema schema,
      final int initialUdafIndex,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList
  ) {
    return create(schema, initialUdafIndex, functionRegistry, functionList, true);
  }

  public AggregateParams create(
      final LogicalSchema schema,
      final int initialUdafIndex,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList
  ) {
    return create(schema, initialUdafIndex, functionRegistry, functionList, false);
  }

  private AggregateParams create(
      final LogicalSchema schema,
      final int initialUdafIndex,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList,
      final boolean table
  ) {
    final List<KsqlAggregateFunction<?, ?, ?>> functions = ImmutableList.copyOf(
        functionList.stream().map(
            funcCall -> UdafUtil.resolveAggregateFunction(
                functionRegistry,
                funcCall,
                schema)
        ).collect(Collectors.toList()));
    final List<Supplier<?>> initialValueSuppliers = functions.stream()
        .map(KsqlAggregateFunction::getInitialValueSupplier)
        .collect(Collectors.toList());
    final Optional<KudafUndoAggregator> undoAggregator;
    if (table) {
      final List<TableAggregationFunction<?, ?, ?>> tableFunctions = new LinkedList<>();
      for (final KsqlAggregateFunction function : functions) {
        tableFunctions.add((TableAggregationFunction<?, ?, ?>) function);
      }
      undoAggregator = Optional.of(new KudafUndoAggregator(initialUdafIndex, tableFunctions));
    } else {
      undoAggregator = Optional.empty();
    }
    return new AggregateParams(
        new KudafInitializer(initialUdafIndex, initialValueSuppliers),
        aggregatorFactory.create(initialUdafIndex, functions),
        undoAggregator,
        new WindowSelectMapper(initialUdafIndex, functions),
        buildSchema(schema, initialUdafIndex, functions, true),
        buildSchema(schema, initialUdafIndex, functions, false)
    );
  }

  private static LogicalSchema buildSchema(
      final LogicalSchema schema,
      final int initialUdafIndex,
      final List<KsqlAggregateFunction<?, ?, ?>> aggregateFunctions,
      final boolean useAggregate) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final List<Column> cols = schema.value();

    schemaBuilder.keyColumns(schema.key());

    for (int i = 0; i < initialUdafIndex; i++) {
      schemaBuilder.valueColumn(cols.get(i));
    }

    for (int i = 0; i < aggregateFunctions.size(); i++) {
      final KsqlAggregateFunction aggregateFunction = aggregateFunctions.get(i);
      final ColumnName colName = ColumnName.aggregateColumn(i);
      final SqlType fieldType = useAggregate
          ? aggregateFunction.getAggregateType()
          : aggregateFunction.returnType();
      schemaBuilder.valueColumn(colName, fieldType);
    }

    return schemaBuilder.build();
  }

  interface KudafAggregatorFactory {
    KudafAggregator create(
        int initialUdafIndex,
        List<KsqlAggregateFunction<?, ?, ?>> functions
    );
  }
}
