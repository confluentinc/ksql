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

package io.confluent.ksql.structured;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.function.TableAggregationFunction;
import io.confluent.ksql.execution.function.UdafUtil;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.TableAggregate;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;

public class SchemaKGroupedTable extends SchemaKGroupedStream {

  private final ExecutionStep<KGroupedTableHolder> sourceTableStep;

  SchemaKGroupedTable(
      final ExecutionStep<KGroupedTableHolder> sourceTableStep,
      final LogicalSchema schema,
      final KeyFormat keyFormat,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    super(
        null,
        schema,
        keyFormat,
        ksqlConfig,
        functionRegistry
    );

    this.sourceTableStep = Objects.requireNonNull(sourceTableStep, "sourceTableStep");
  }

  public ExecutionStep<KGroupedTableHolder> getSourceTableStep() {
    return sourceTableStep;
  }

  @Override
  public SchemaKTable<Struct> aggregate(
      final List<ColumnName> nonAggregateColumns,
      final List<FunctionCall> aggregations,
      final Optional<WindowExpression> windowExpression,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker
  ) {
    if (windowExpression.isPresent()) {
      throw new KsqlException("Windowing not supported for table aggregations.");
    }

    final List<String> unsupportedFunctionNames = aggregations.stream()
        .map(call -> UdafUtil.resolveAggregateFunction(functionRegistry, call, schema))
        .filter(function -> !(function instanceof TableAggregationFunction))
        .map(KsqlAggregateFunction::name)
        .map(FunctionName::text)
        .distinct()
        .collect(Collectors.toList());

    if (!unsupportedFunctionNames.isEmpty()) {
      final String postfix = unsupportedFunctionNames.size() == 1 ? "" : "s";
      throw new KsqlException("The aggregation function" + postfix + " "
          + GrammaticalJoiner.and().join(unsupportedFunctionNames)
          + " cannot be applied to a table source, only to a stream source."
      );
    }

    final TableAggregate step = ExecutionStepFactory.tableAggregate(
        contextStacker,
        sourceTableStep,
        Formats.of(keyFormat, valueFormat, SerdeOptions.of()),
        nonAggregateColumns,
        aggregations
    );

    return new SchemaKTable<>(
        step,
        resolveSchema(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }
}
