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
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AggregateParamsFactory {

  private final KudafAggregatorFactory aggregatorFactory;
  private final KudafUndoAggregatorFactory undoAggregatorFactory;

  public AggregateParamsFactory() {
    this(KudafAggregator::new, KudafUndoAggregator::new);
  }

  @VisibleForTesting
  AggregateParamsFactory(
      final KudafAggregatorFactory aggregatorFactory,
      final KudafUndoAggregatorFactory undoAggregatorFactory
  ) {
    this.aggregatorFactory = Objects.requireNonNull(aggregatorFactory);
    this.undoAggregatorFactory = Objects.requireNonNull(undoAggregatorFactory);
  }

  public AggregateParams createUndoable(
      final LogicalSchema schema,
      final List<ColumnName> nonAggregateColumns,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList,
      final KsqlConfig config
  ) {
    return create(schema, nonAggregateColumns, functionRegistry, functionList, true, false, config);
  }

  public AggregateParams create(
      final LogicalSchema schema,
      final List<ColumnName> nonAggregateColumns,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList,
      final boolean windowedAggregation,
      final KsqlConfig config
  ) {
    return create(
        schema,
        nonAggregateColumns,
        functionRegistry,
        functionList,
        false,
        windowedAggregation,
        config
    );
  }

  private AggregateParams create(
      final LogicalSchema schema,
      final List<ColumnName> nonAggregateColumns,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList,
      final boolean table,
      final boolean windowedAggregation,
      final KsqlConfig config
  ) {
    final List<KsqlAggregateFunction<?, ?, ?>> functions =
        resolveAggregateFunctions(schema, functionRegistry, functionList, config);

    final List<Supplier<?>> initialValueSuppliers = functions.stream()
        .map(KsqlAggregateFunction::getInitialValueSupplier)
        .collect(Collectors.toList());

    final Optional<KudafUndoAggregator> undoAggregator =
        buildUndoAggregators(nonAggregateColumns.size(), table, functions);

    final LogicalSchema aggregateSchema =
        buildSchema(schema, nonAggregateColumns, functions, true, false);

    final LogicalSchema outputSchema =
        buildSchema(schema, nonAggregateColumns, functions, false, windowedAggregation);

    return new AggregateParams(
        new KudafInitializer(nonAggregateColumns.size(), initialValueSuppliers),
        aggregatorFactory.create(nonAggregateColumns.size(), functions),
        undoAggregator,
        aggregateSchema,
        outputSchema
    );
  }

  private Optional<KudafUndoAggregator> buildUndoAggregators(
      final int nonAggColumnCount,
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
    return Optional.of(undoAggregatorFactory.create(nonAggColumnCount, tableFunctions));
  }

  private static List<KsqlAggregateFunction<?, ?, ?>> resolveAggregateFunctions(
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry,
      final List<FunctionCall> functionList,
      final KsqlConfig config
  ) {
    return ImmutableList.copyOf(
        functionList.stream().map(
            funcCall -> UdafUtil.resolveAggregateFunction(
                functionRegistry,
                funcCall,
                schema,
                config
            )
        ).collect(Collectors.toList()));
  }

  private static LogicalSchema buildSchema(
      final LogicalSchema schema,
      final List<ColumnName> nonAggregateColumns,
      final List<KsqlAggregateFunction<?, ?, ?>> aggregateFunctions,
      final boolean useAggregate,
      final boolean addWindowBounds
  ) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();

    schemaBuilder.keyColumns(schema.key());

    for (final ColumnName columnName : nonAggregateColumns) {
      final Column col = schema.findValueColumn(columnName)
          .orElseThrow(IllegalArgumentException::new);

      schemaBuilder.valueColumn(col);
    }

    for (int i = 0; i < aggregateFunctions.size(); i++) {
      final KsqlAggregateFunction<?, ?, ?> aggregateFunction = aggregateFunctions.get(i);
      final ColumnName colName = ColumnNames.aggregateColumn(i);
      final SqlType fieldType = useAggregate
          ? aggregateFunction.getAggregateType()
          : aggregateFunction.returnType();
      schemaBuilder.valueColumn(colName, fieldType);
    }

    if (addWindowBounds) {
      // Add window bounds columns, as populated by WindowBoundsPopulator
      schemaBuilder.valueColumn(SystemColumns.WINDOWSTART_NAME, SystemColumns.WINDOWBOUND_TYPE);
      schemaBuilder.valueColumn(SystemColumns.WINDOWEND_NAME, SystemColumns.WINDOWBOUND_TYPE);
    }

    return schemaBuilder.build();
  }

  interface KudafAggregatorFactory {

    KudafAggregator<?> create(
        int nonAggColumnCount,
        List<KsqlAggregateFunction<?, ?, ?>> functions
    );
  }

  interface KudafUndoAggregatorFactory {

    KudafUndoAggregator create(
        int nonAggColumnCount,
        List<TableAggregationFunction<?, ?, ?>> functions
    );
  }
}
