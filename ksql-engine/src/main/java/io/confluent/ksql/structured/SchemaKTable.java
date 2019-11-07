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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.plan.TableGroupBy;
import io.confluent.ksql.execution.plan.TableMapValues;
import io.confluent.ksql.execution.plan.TableSink;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class SchemaKTable<K> extends SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final ExecutionStep<KTableHolder<K>> sourceTableStep;

  public SchemaKTable(
      final ExecutionStep<KTableHolder<K>> sourceTableStep,
      final KeyFormat keyFormat,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Type type,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    super(
        null,
        Objects.requireNonNull(sourceTableStep, "sourceTableStep").getProperties(),
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        type,
        ksqlConfig,
        functionRegistry
    );
    this.sourceTableStep = sourceTableStep;
  }

  @Override
  public SchemaKTable<K> into(
      final String kafkaTopicName,
      final LogicalSchema outputSchema,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options,
      final QueryContext.Stacker contextStacker
  ) {
    final TableSink<K> step = ExecutionStepFactory.tableSink(
        contextStacker,
        outputSchema,
        sourceTableStep,
        Formats.of(keyFormat, valueFormat, options),
        kafkaTopicName
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        type,
        ksqlConfig,
        functionRegistry
    );
  }

  @Override
  public SchemaKTable<K> filter(
      final Expression filterExpression,
      final QueryContext.Stacker contextStacker
  ) {
    final TableFilter<K> step = ExecutionStepFactory.tableFilter(
        contextStacker,
        sourceTableStep,
        rewriteTimeComparisonForFilter(filterExpression)
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        Collections.singletonList(this),
        Type.FILTER,
        ksqlConfig,
        functionRegistry
    );
  }

  @Override
  public SchemaKTable<K> select(
      final List<SelectExpression> selectExpressions,
      final QueryContext.Stacker contextStacker,
      final KsqlQueryBuilder ksqlQueryBuilder) {
    final KeyField keyField = findKeyField(selectExpressions);
    final TableMapValues<K> step = ExecutionStepFactory.tableMapValues(
        contextStacker,
        sourceTableStep,
        selectExpressions,
        ksqlQueryBuilder
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        Collections.singletonList(this),
        Type.PROJECT,
        ksqlConfig,
        functionRegistry
    );
  }

  @Override
  public ExecutionStep<?> getSourceStep() {
    return sourceTableStep;
  }

  public ExecutionStep<KTableHolder<K>> getSourceTableStep() {
    return sourceTableStep;
  }

  @Override
  public SchemaKGroupedTable groupBy(
      final ValueFormat valueFormat,
      final List<Expression> groupByExpressions,
      final QueryContext.Stacker contextStacker
  ) {

    final KeyFormat groupedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());

    final ColumnRef aggregateKeyName = groupedKeyNameFor(groupByExpressions);
    final Optional<ColumnRef> newKeyField = getSchema()
        .findValueColumn(aggregateKeyName.withoutSource())
        .map(Column::ref);

    final TableGroupBy<K> step = ExecutionStepFactory.tableGroupBy(
        contextStacker,
        sourceTableStep,
        Formats.of(groupedKeyFormat, valueFormat, SerdeOption.none()),
        groupByExpressions
    );
    return new SchemaKGroupedTable(
        step,
        groupedKeyFormat,
        KeyField.of(newKeyField),
        Collections.singletonList(this),
        ksqlConfig,
        functionRegistry);
  }

  public SchemaKTable<K> join(
      final SchemaKTable<K> schemaKTable,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final QueryContext.Stacker contextStacker
  ) {
    final TableTableJoin<K> step = ExecutionStepFactory.tableTableJoin(
        contextStacker,
        JoinType.INNER,
        sourceTableStep,
        schemaKTable.getSourceTableStep(),
        joinSchema
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKTable<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final QueryContext.Stacker contextStacker
  ) {
    final TableTableJoin<K> step = ExecutionStepFactory.tableTableJoin(
        contextStacker,
        JoinType.LEFT,
        sourceTableStep,
        schemaKTable.getSourceTableStep(),
        joinSchema
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKTable<K> outerJoin(
      final SchemaKTable<K> schemaKTable,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final QueryContext.Stacker contextStacker
  ) {
    final TableTableJoin<K> step = ExecutionStepFactory.tableTableJoin(
        contextStacker,
        JoinType.OUTER,
        sourceTableStep,
        schemaKTable.getSourceTableStep(),
        joinSchema
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        ksqlConfig,
        functionRegistry
    );
  }
}
