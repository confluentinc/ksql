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

import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ForeignKeyTableTableJoin;
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
import io.confluent.ksql.planner.plan.PlanBuildContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.InternalFormats;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.serde.SerdeFeaturesFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Optional;

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
      final KsqlTopic topic,
      final QueryContext.Stacker contextStacker,
      final Optional<TimestampColumn> timestampColumn
  ) {
    if (!keyFormat.getWindowInfo().equals(topic.getKeyFormat().getWindowInfo())) {
      throw new IllegalArgumentException("Can't change windowing");
    }

    final TableSink<K> step = ExecutionStepFactory.tableSink(
        contextStacker,
        sourceTableStep,
        Formats.from(topic),
        topic.getKafkaTopicName(),
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
      final PlanBuildContext buildContext,
      final FormatInfo valueFormat
  ) {
    final TableSelect<K> step = ExecutionStepFactory.tableMapValues(
        contextStacker,
        sourceTableStep,
        keyColumnNames,
        selectExpressions,
        InternalFormats.of(keyFormat, valueFormat)
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
  public SchemaKTable<K> selectKey(
      final FormatInfo valueFormat,
      final List<Expression> keyExpression,
      final Optional<KeyFormat> forceInternalKeyFormat,
      final Stacker contextStacker,
      final boolean forceRepartition
  ) {
    final boolean repartitionNeeded = repartitionNeeded(keyExpression);
    final boolean keyFormatChange = forceInternalKeyFormat.isPresent()
        && !forceInternalKeyFormat.get().equals(keyFormat);

    if (!forceRepartition && !keyFormatChange && !repartitionNeeded) {
      return this;
    }

    if (schema.key().size() > 1) {
      // let's throw a better error message in the case of multi-column tables
      throw new UnsupportedOperationException("Cannot repartition a TABLE source. If this is "
          + "a join, joins on tables with multiple columns is not yet supported.");
    }

    // Table repartitioning is only supported for internal use in enabling joins
    // where we know that the key will be semantically equivalent, but may be serialized
    // differently (thus ensuring all keys are routed to the same partitions)
    if (repartitionNeeded) {
      throw new UnsupportedOperationException("Cannot repartition a TABLE source. "
          + "If this is a join, make sure that the criteria uses the TABLE's key column "
          + Iterables.getOnlyElement(schema.key()).name().text() + " instead of "
          + keyExpression);
    }

    if (keyFormat.isWindowed()) {
      final String errorMsg = "Implicit repartitioning of windowed sources is not supported. "
          + "See https://github.com/confluentinc/ksql/issues/4385.";
      final String additionalMsg = forceRepartition
          ? " As a result, ksqlDB does not support joins on windowed sources with "
          + "Schema-Registry-enabled key formats (AVRO, JSON_SR, PROTOBUF) at this time. "
          + "Please repartition your sources to use a different key format before performing "
          + "the join."
          : "";
      throw new KsqlException(errorMsg + additionalMsg);
    }

    final KeyFormat newKeyFormat = SerdeFeaturesFactory.sanitizeKeyFormat(
        forceInternalKeyFormat.orElse(keyFormat),
        toSqlTypes(keyExpression),
        false // logical schema changes are not supported
    );

    final ExecutionStep<KTableHolder<K>> step = ExecutionStepFactory.tableSelectKey(
        contextStacker,
        sourceTableStep,
        InternalFormats.of(newKeyFormat, valueFormat),
        keyExpression
    );

    return new SchemaKTable<>(
        step,
        resolveSchema(step),
        newKeyFormat,
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
      final FormatInfo valueFormat,
      final List<Expression> groupByExpressions,
      final Stacker contextStacker
  ) {
    // Since tables must have a key, we know that the keyFormat is both
    // not NONE and has at least one column; this allows us to inherit
    // the key format directly (as opposed to the logic in SchemaKStream)
    final KeyFormat groupedKeyFormat = SerdeFeaturesFactory.sanitizeKeyFormat(
        KeyFormat.nonWindowed(keyFormat.getFormatInfo(), keyFormat.getFeatures()),
        toSqlTypes(groupByExpressions),
        true
    );

    final TableGroupBy<K> step = ExecutionStepFactory.tableGroupBy(
        contextStacker,
        sourceTableStep,
        InternalFormats.of(groupedKeyFormat, valueFormat),
        groupByExpressions
    );

    return new SchemaKGroupedTable(
        step,
        resolveSchema(step),
        groupedKeyFormat,
        ksqlConfig,
        functionRegistry);
  }

  public SchemaKTable<K> innerJoin(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final Stacker contextStacker
  ) {
    return join(schemaKTable, keyColName, contextStacker, JoinType.INNER);
  }

  public SchemaKTable<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final Stacker contextStacker
  ) {
    return join(schemaKTable, keyColName, contextStacker, JoinType.LEFT);
  }

  public SchemaKTable<K> rightJoin(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final Stacker contextStacker
  ) {
    return join(schemaKTable, keyColName, contextStacker, JoinType.RIGHT);
  }

  public SchemaKTable<K> outerJoin(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final QueryContext.Stacker contextStacker
  ) {
    return join(schemaKTable, keyColName, contextStacker, JoinType.OUTER);
  }

  public <KRightT> SchemaKTable<K> foreignKeyInnerJoin(
      final SchemaKTable<KRightT> schemaKTable,
      final Optional<ColumnName> leftJoinColumnName,
      final Optional<Expression> leftJoinExpression,
      final Stacker contextStacker,
      final FormatInfo valueFormatInfo
  ) {
    return foreignKeyJoin(
        schemaKTable,
        leftJoinColumnName,
        leftJoinExpression,
        contextStacker,
        valueFormatInfo,
        JoinType.INNER);
  }

  public <KRightT> SchemaKTable<K> foreignKeyLeftJoin(
      final SchemaKTable<KRightT> schemaKTable,
      final Optional<ColumnName> leftJoinColumnName,
      final Optional<Expression> leftJoinExpression,
      final Stacker contextStacker,
      final FormatInfo valueFormatInfo
  ) {
    return foreignKeyJoin(
        schemaKTable,
        leftJoinColumnName,
        leftJoinExpression,
        contextStacker,
        valueFormatInfo,
        JoinType.LEFT);
  }

  public SchemaKTable<K> suppress(
      final RefinementInfo refinementInfo,
      final FormatInfo valueFormat,
      final Stacker contextStacker
  ) {
    final TableSuppress<K> step = ExecutionStepFactory.tableSuppress(
        contextStacker,
        sourceTableStep,
        refinementInfo,
        InternalFormats.of(keyFormat, valueFormat)
    );

    return new SchemaKTable<>(
        step,
        resolveSchema(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  private SchemaKTable<K> join(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final Stacker contextStacker,
      final JoinType joinType
  ) {
    throwOnJoinKeyFormatsMismatch(schemaKTable);

    final TableTableJoin<K> step = ExecutionStepFactory.tableTableJoin(
        contextStacker,
        joinType,
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

  private  <KRightT> SchemaKTable<K> foreignKeyJoin(
      final SchemaKTable<KRightT> schemaKTable,
      final Optional<ColumnName> leftJoinColumnName,
      final Optional<Expression> leftJoinExpression,
      final Stacker contextStacker,
      final FormatInfo valueFormatInfo,
      final JoinType joinType
  ) {
    final ForeignKeyTableTableJoin<K, KRightT> step =
        ExecutionStepFactory.foreignKeyTableTableJoin(
            contextStacker,
            joinType,
            leftJoinColumnName,
            InternalFormats.of(keyFormat, valueFormatInfo),
            sourceTableStep,
            schemaKTable.getSourceTableStep(),
            leftJoinExpression
        );

    return new SchemaKTable<>(
        step,
        resolveSchema(step, schemaKTable),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }
}
