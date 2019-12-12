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
import io.confluent.ksql.execution.transform.window.WindowSelectMapper;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.ArrayList;
import java.util.Collections;
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
      final List<ColumnRef> nonAggregateColumns,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList
  ) {
    return create(schema, nonAggregateColumns, functionRegistry, functionList, true);
  }

  public AggregateParams create(
      final LogicalSchema schema,
      final List<ColumnRef> nonAggregateColumns,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList
  ) {
    return create(schema, nonAggregateColumns, functionRegistry, functionList, false);
  }

  private AggregateParams create(
      final LogicalSchema schema,
      final List<ColumnRef> nonAggregateColumns,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList,
      final boolean table
  ) {
    final List<Integer> nonAggColumnIndexes = nonAggColumnIndexes(schema, nonAggregateColumns);

    final List<KsqlAggregateFunction<?, ?, ?>> functions =
        resolveAggregateFunctions(schema, functionRegistry, functionList);

    final List<Supplier<?>> initialValueSuppliers = functions.stream()
        .map(KsqlAggregateFunction::getInitialValueSupplier)
        .collect(Collectors.toList());

    final Optional<KudafUndoAggregator> undoAggregator =
        buildUndoAggregators(nonAggColumnIndexes, table, functions);

    final LogicalSchema aggregateSchema = buildSchema(schema, nonAggregateColumns, functions, true);

    final LogicalSchema outputSchema = buildSchema(schema, nonAggregateColumns, functions, false);

    return new AggregateParams(
        new KudafInitializer(nonAggregateColumns.size(), initialValueSuppliers),
        aggregatorFactory.create(nonAggColumnIndexes, functions),
        undoAggregator,
        new WindowSelectMapper(nonAggregateColumns.size(), functions),
        aggregateSchema,
        outputSchema
    );
  }

  private static List<Integer> nonAggColumnIndexes(
      final LogicalSchema schema,
      final List<ColumnRef> nonAggregateColumns
  ) {
    final List<Integer> indexes = new ArrayList<>(nonAggregateColumns.size());
    for (final ColumnRef columnRef : nonAggregateColumns) {
      indexes.add(schema.findValueColumn(columnRef).map(Column::index).orElseThrow(
          () -> new IllegalStateException("invalid column ref: "  + columnRef)
      ));
    }
    return Collections.unmodifiableList(indexes);
  }

  private static Optional<KudafUndoAggregator> buildUndoAggregators(
      final List<Integer> nonAggColumnIndexes,
      final boolean table,
      final List<KsqlAggregateFunction<?, ?, ?>> functions
  ) {
    if (!table) {
      return Optional.empty();
    }

    final List<TableAggregationFunction<?, ?, ?>> tableFunctions = new LinkedList<>();
    for (final KsqlAggregateFunction<?, ?, ?> function : functions) {
      tableFunctions.add((TableAggregationFunction<?, ?, ?>) function);
    }
    return Optional.of(new KudafUndoAggregator(nonAggColumnIndexes, tableFunctions));
  }

  private static List<KsqlAggregateFunction<?, ?, ?>> resolveAggregateFunctions(
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList
  ) {
    return ImmutableList.copyOf(
        functionList.stream().map(
            funcCall -> UdafUtil.resolveAggregateFunction(
                functionRegistry,
                funcCall,
                schema)
        ).collect(Collectors.toList()));
  }

  private static LogicalSchema buildSchema(
      final LogicalSchema schema,
      final List<ColumnRef> nonAggregateColumns,
      final List<KsqlAggregateFunction<?, ?, ?>> aggregateFunctions,
      final boolean useAggregate
  ) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();

    schemaBuilder.keyColumns(schema.key());

    for (final ColumnRef columnRef : nonAggregateColumns) {
      final Column col = schema.findValueColumn(columnRef)
          .orElseThrow(IllegalArgumentException::new);

      schemaBuilder.valueColumn(col);
    }

    for (int i = 0; i < aggregateFunctions.size(); i++) {
      final KsqlAggregateFunction<?, ?, ?> aggregateFunction = aggregateFunctions.get(i);
      final ColumnName colName = ColumnName.aggregateColumn(i);
      final SqlType fieldType = useAggregate
          ? aggregateFunction.getAggregateType()
          : aggregateFunction.returnType();
      schemaBuilder.valueColumn(colName, fieldType);
    }

    return schemaBuilder.build();
  }

  interface KudafAggregatorFactory {
    KudafAggregator<?> create(
        List<Integer> nonAggColumnIndexes,
        List<KsqlAggregateFunction<?, ?, ?>> functions
    );
  }
}
