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
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
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
      final KeyFormat keyFormat,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    super(
        null,
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        ksqlConfig,
        functionRegistry
    );

    this.sourceTableStep = Objects.requireNonNull(sourceTableStep, "sourceTableStep");
  }

  public ExecutionStep<KGroupedTableHolder> getSourceTableStep() {
    return sourceTableStep;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable<Struct> aggregate(
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregations,
      final Optional<WindowExpression> windowExpression,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker
  ) {
    if (windowExpression.isPresent()) {
      throw new KsqlException("Windowing not supported for table aggregations.");
    }

    final List<String> unsupportedFunctionNames = aggregations.stream()
        .map(call -> UdafUtil.resolveAggregateFunction(
            functionRegistry, call, sourceTableStep.getSchema())
        ).filter(function -> !(function instanceof TableAggregationFunction))
        .map(KsqlAggregateFunction::name)
        .map(FunctionName::name)
        .collect(Collectors.toList());
    if (!unsupportedFunctionNames.isEmpty()) {
      throw new KsqlException(
          String.format(
            "The aggregation function(s) (%s) cannot be applied to a table.",
            String.join(", ", unsupportedFunctionNames)));
    }

    final TableAggregate step = ExecutionStepFactory.tableAggregate(
        contextStacker,
        sourceTableStep,
        Formats.of(keyFormat, valueFormat, SerdeOption.none()),
        nonFuncColumnCount,
        aggregations,
        functionRegistry
    );

    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        ksqlConfig,
        functionRegistry
    );
  }
}
