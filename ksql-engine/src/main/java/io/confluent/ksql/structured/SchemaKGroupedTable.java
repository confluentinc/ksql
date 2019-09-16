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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.MaterializedFactory;
import io.confluent.ksql.execution.streams.StreamsUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.TableAggregationFunction;
import io.confluent.ksql.function.udaf.KudafAggregator;
import io.confluent.ksql.function.udaf.KudafUndoAggregator;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

public class SchemaKGroupedTable extends SchemaKGroupedStream {
  private final KGroupedTable kgroupedTable;
  private final ExecutionStep<KGroupedTable<Struct, GenericRow>> sourceTableStep;

  SchemaKGroupedTable(
      final KGroupedTable kgroupedTable,
      final ExecutionStep<KGroupedTable<Struct, GenericRow>> sourceTableStep,
      final KeyFormat keyFormat,
      final KeySerde<Struct> keySerde,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this(
        kgroupedTable,
        sourceTableStep,
        keyFormat,
        keySerde,
        keyField,
        sourceSchemaKStreams,
        ksqlConfig,
        functionRegistry,
        MaterializedFactory.create(ksqlConfig));
  }

  SchemaKGroupedTable(
      final KGroupedTable kgroupedTable,
      final ExecutionStep<KGroupedTable<Struct, GenericRow>> sourceTableStep,
      final KeyFormat keyFormat,
      final KeySerde<Struct> keySerde,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final MaterializedFactory materializedFactory
  ) {
    super(
        null,
        null,
        keyFormat,
        keySerde,
        keyField,
        sourceSchemaKStreams,
        ksqlConfig,
        functionRegistry,
        materializedFactory
    );

    this.kgroupedTable = Objects.requireNonNull(kgroupedTable, "kgroupedTable");
    this.sourceTableStep = Objects.requireNonNull(sourceTableStep, "sourceTableStep");
  }

  public ExecutionStep<KGroupedTable<Struct, GenericRow>> getSourceTableStep() {
    return sourceTableStep;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable<Struct> aggregate(
      final LogicalSchema aggregateSchema,
      final Initializer initializer,
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregations,
      final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap,
      final WindowExpression windowExpression,
      final ValueFormat valueFormat,
      final Serde<GenericRow> topicValueSerDe,
      final QueryContext.Stacker contextStacker
  ) {
    if (windowExpression != null) {
      throw new KsqlException("Windowing not supported for table aggregations.");
    }

    throwOnValueFieldCountMismatch(aggregateSchema, nonFuncColumnCount, aggValToFunctionMap);

    final List<String> unsupportedFunctionNames = aggValToFunctionMap.values()
        .stream()
        .filter(function -> !(function instanceof TableAggregationFunction))
        .map(KsqlAggregateFunction::getFunctionName)
        .collect(Collectors.toList());
    if (!unsupportedFunctionNames.isEmpty()) {
      throw new KsqlException(
          String.format(
            "The aggregation function(s) (%s) cannot be applied to a table.",
            String.join(", ", unsupportedFunctionNames)));
    }

    final KudafAggregator aggregator = new KudafAggregator(
        nonFuncColumnCount, aggValToFunctionMap);

    final Map<Integer, TableAggregationFunction> aggValToUndoFunctionMap =
        aggValToFunctionMap.keySet()
            .stream()
            .collect(
                Collectors.toMap(
                    k -> k,
                    k -> ((TableAggregationFunction) aggValToFunctionMap.get(k))));

    final KudafUndoAggregator subtractor = new KudafUndoAggregator(
        nonFuncColumnCount, aggValToUndoFunctionMap);

    final Materialized<Struct, GenericRow, ?> materialized = materializedFactory.create(
        keySerde,
        topicValueSerDe,
        StreamsUtil.buildOpName(contextStacker.getQueryContext())
    );

    final KTable<Struct, GenericRow> aggKtable = kgroupedTable.aggregate(
        initializer,
        aggregator,
        subtractor,
        materialized);

    final ExecutionStep step = ExecutionStepFactory.tableAggregate(
        contextStacker,
        sourceTableStep,
        aggregateSchema,
        Formats.of(keyFormat, valueFormat, SerdeOption.none()),
        nonFuncColumnCount,
        aggregations
    );
    return new SchemaKTable<>(
        aggKtable,
        step,
        keyFormat,
        keySerde,
        keyField,
        sourceSchemaKStreams,
        SchemaKStream.Type.AGGREGATE,
        ksqlConfig,
        functionRegistry
    );
  }
}
