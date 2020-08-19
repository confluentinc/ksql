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
import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.plan.TableGroupBy;
import io.confluent.ksql.execution.plan.TableSelect;
import io.confluent.ksql.execution.plan.TableSink;
import io.confluent.ksql.execution.plan.TableSuppress;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class SchemaKTable<K> extends SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final ExecutionStep<KTableHolder<K>> sourceTableStep;

  public SchemaKTable(
      final ExecutionStep<KTableHolder<K>> sourceTableStep,
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
    this.sourceTableStep = sourceTableStep;
  }

  @Override
  public SchemaKTable<K> into(
      final String kafkaTopicName,
      final ValueFormat valueFormat,
      final SerdeOptions options,
      final QueryContext.Stacker contextStacker,
      final Optional<TimestampColumn> timestampColumn
  ) {
    final TableSink<K> step = ExecutionStepFactory.tableSink(
        contextStacker,
        sourceTableStep,
        Formats.of(keyFormat, valueFormat, options),
        kafkaTopicName,
        timestampColumn
    );
    return new SchemaKTable<>(
        step,
        resolveSchema(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  @Override
  public SchemaKTable<K> filter(
      final Expression filterExpression,
      final Stacker contextStacker
  ) {
    final TableFilter<K> step = ExecutionStepFactory.tableFilter(
        contextStacker,
        sourceTableStep,
        rewriteTimeComparisonForFilter(filterExpression)
    );

    return new SchemaKTable<>(
        step,
        resolveSchema(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  @Override
  public SchemaKTable<K> select(
      final List<ColumnName> keyColumnNames,
      final List<SelectExpression> selectExpressions,
      final Stacker contextStacker,
      final KsqlQueryBuilder ksqlQueryBuilder
  ) {
    final TableSelect<K> step = ExecutionStepFactory.tableMapValues(
        contextStacker,
        sourceTableStep,
        keyColumnNames,
        selectExpressions
    );

    return new SchemaKTable<>(
        step,
        resolveSchema(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKStream<Struct> selectKey(
      final Expression keyExpression,
      final Stacker contextStacker
  ) {
    if (repartitionNotNeeded(ImmutableList.of(keyExpression))) {
      return (SchemaKStream<Struct>) this;
    }

    throw new UnsupportedOperationException("Cannot repartition a TABLE source. "
        + "If this is a join, make sure that the criteria uses the TABLE's key column "
        + Iterables.getOnlyElement(schema.key()).name().text() + " instead of "
        + keyExpression);
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
      final Stacker contextStacker
  ) {
    final KeyFormat groupedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());

    final TableGroupBy<K> step = ExecutionStepFactory.tableGroupBy(
        contextStacker,
        sourceTableStep,
        Formats.of(groupedKeyFormat, valueFormat, SerdeOptions.of()),
        groupByExpressions
    );

    return new SchemaKGroupedTable(
        step,
        resolveSchema(step),
        groupedKeyFormat,
        ksqlConfig,
        functionRegistry);
  }

  public SchemaKTable<K> join(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final Stacker contextStacker
  ) {
    final TableTableJoin<K> step = ExecutionStepFactory.tableTableJoin(
        contextStacker,
        JoinType.INNER,
        keyColName,
        sourceTableStep,
        schemaKTable.getSourceTableStep()
    );
    return new SchemaKTable<>(
        step,
        resolveSchema(step, schemaKTable),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKTable<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final Stacker contextStacker
  ) {
    final TableTableJoin<K> step = ExecutionStepFactory.tableTableJoin(
        contextStacker,
        JoinType.LEFT,
        keyColName,
        sourceTableStep,
        schemaKTable.getSourceTableStep()
    );
    return new SchemaKTable<>(
        step,
        resolveSchema(step, schemaKTable),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKTable<K> outerJoin(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final QueryContext.Stacker contextStacker
  ) {
    final TableTableJoin<K> step = ExecutionStepFactory.tableTableJoin(
        contextStacker,
        JoinType.OUTER,
        keyColName,
        sourceTableStep,
        schemaKTable.getSourceTableStep()
    );
    return new SchemaKTable<>(
        step,
        resolveSchema(step, schemaKTable),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKTable<K> suppress(
      final RefinementInfo refinementInfo,
      final ValueFormat valueFormat,
      final Stacker contextStacker
  ) {
    final TableSuppress<K> step = ExecutionStepFactory.tableSuppress(
        contextStacker,
        sourceTableStep,
        refinementInfo,
        Formats.of(keyFormat, valueFormat, SerdeOptions.of())
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
