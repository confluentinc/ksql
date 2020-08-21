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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.engine.rewrite.StatementRewriteForMagicPseudoTimestamp;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.plan.StreamFlatMap;
import io.confluent.ksql.execution.plan.StreamGroupBy;
import io.confluent.ksql.execution.plan.StreamGroupByKey;
import io.confluent.ksql.execution.plan.StreamSelect;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.JoinWindows;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final String GROUP_BY_COLUMN_SEPARATOR = "|+|";

  public enum Type { SOURCE, PROJECT, FILTER, AGGREGATE, SINK, REKEY, JOIN }

  final KeyFormat keyFormat;
  final KsqlConfig ksqlConfig;
  final FunctionRegistry functionRegistry;
  final LogicalSchema schema;
  private final ExecutionStep<KStreamHolder<K>> sourceStep;

  SchemaKStream(
      final ExecutionStep<KStreamHolder<K>> sourceStep,
      final LogicalSchema schema,
      final KeyFormat keyFormat,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this.keyFormat = requireNonNull(keyFormat, "keyFormat");
    this.sourceStep = sourceStep;
    this.schema = Objects.requireNonNull(schema, "schema");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
  }

  public SchemaKStream<K> into(
      final String kafkaTopicName,
      final ValueFormat valueFormat,
      final SerdeOptions options,
      final QueryContext.Stacker contextStacker,
      final Optional<TimestampColumn> timestampColumn
  ) {
    final StreamSink<K> step = ExecutionStepFactory.streamSink(
        contextStacker,
        Formats.of(keyFormat, valueFormat, options),
        sourceStep,
        kafkaTopicName,
        timestampColumn
    );
    return new SchemaKStream<>(
        step,
        resolveSchema(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> filter(
      final Expression filterExpression,
      final Stacker contextStacker
  ) {
    final StreamFilter<K> step = ExecutionStepFactory.streamFilter(
        contextStacker,
        sourceStep,
        rewriteTimeComparisonForFilter(filterExpression)
    );

    return new SchemaKStream<>(
        step,
        resolveSchema(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  static Expression rewriteTimeComparisonForFilter(final Expression expression) {
    return new StatementRewriteForMagicPseudoTimestamp().rewrite(expression);
  }

  public SchemaKStream<K> select(
      final List<ColumnName> keyColumnNames,
      final List<SelectExpression> selectExpressions,
      final Stacker contextStacker,
      final KsqlQueryBuilder ksqlQueryBuilder
  ) {
    final StreamSelect<K> step = ExecutionStepFactory.streamSelect(
        contextStacker,
        sourceStep,
        keyColumnNames,
        selectExpressions
    );

    return new SchemaKStream<>(
        step,
        resolveSchema(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final ValueFormat valueFormat,
      final Stacker contextStacker
  ) {
    final StreamTableJoin<K> step = ExecutionStepFactory.streamTableJoin(
        contextStacker,
        JoinType.LEFT,
        keyColName,
        Formats.of(keyFormat, valueFormat, SerdeOptions.of()),
        sourceStep,
        schemaKTable.getSourceTableStep()
    );

    return new SchemaKStream<>(
        step,
        resolveSchema(step, schemaKTable),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> leftJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final ColumnName keyColName,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
      final Stacker contextStacker
  ) {
    final StreamStreamJoin<K> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.LEFT,
        keyColName,
        Formats.of(keyFormat, leftFormat, SerdeOptions.of()),
        Formats.of(keyFormat, rightFormat, SerdeOptions.of()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinWindows
    );

    return new SchemaKStream<>(
        step,
        resolveSchema(step, otherSchemaKStream),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> join(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final ValueFormat valueFormat,
      final Stacker contextStacker
  ) {
    final StreamTableJoin<K> step = ExecutionStepFactory.streamTableJoin(
        contextStacker,
        JoinType.INNER,
        keyColName,
        Formats.of(keyFormat, valueFormat, SerdeOptions.of()),
        sourceStep,
        schemaKTable.getSourceTableStep()
    );

    return new SchemaKStream<>(
        step,
        resolveSchema(step, schemaKTable),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> join(
      final SchemaKStream<K> otherSchemaKStream,
      final ColumnName keyColName,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
      final Stacker contextStacker
  ) {
    final StreamStreamJoin<K> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.INNER,
        keyColName,
        Formats.of(keyFormat, leftFormat, SerdeOptions.of()),
        Formats.of(keyFormat, rightFormat, SerdeOptions.of()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinWindows
    );

    return new SchemaKStream<>(
        step,
        resolveSchema(step, otherSchemaKStream),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> outerJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final ColumnName keyColName,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
      final Stacker contextStacker
  ) {
    final StreamStreamJoin<K> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.OUTER,
        keyColName,
        Formats.of(keyFormat, leftFormat, SerdeOptions.of()),
        Formats.of(keyFormat, rightFormat, SerdeOptions.of()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinWindows
    );

    return new SchemaKStream<>(
        step,
        resolveSchema(step, otherSchemaKStream),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKStream<Struct> selectKey(
      final Expression keyExpression,
      final Stacker contextStacker
  ) {
    if (repartitionNotNeeded(ImmutableList.of(keyExpression))) {
      return (SchemaKStream<Struct>) this;
    }

    if (keyFormat.isWindowed()) {
      throw new KsqlException("Implicit repartitioning of windowed sources is not supported. "
          + "See https://github.com/confluentinc/ksql/issues/4385.");
    }

    final ExecutionStep<KStreamHolder<Struct>> step = ExecutionStepFactory
        .streamSelectKey(contextStacker, sourceStep, keyExpression);

    return new SchemaKStream<>(
        step,
        resolveSchema(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  boolean repartitionNotNeeded(final List<Expression> expressions) {
    // Note: A repartition is only not required if partitioning by the existing key column, or
    // the existing keyField.

    if (schema.key().isEmpty()) {
      // No current key, so repartition needed:
      return false;
    }

    if (schema.key().size() != 1) {
      throw new UnsupportedOperationException("logic only supports single key column");
    }

    if (expressions.size() != 1) {
      // Currently only support single key column,
      // so a repartition on multiple expressions _must_ require a re-key
      return false;
    }

    final Expression expression = expressions.get(0);
    if (!(expression instanceof ColumnReferenceExp)) {
      // If expression is not a column reference then the key will be changing
      return false;
    }

    final ColumnName newKeyColName = ((ColumnReferenceExp) expression).getColumnName();

    getSchema()
        .findValueColumn(newKeyColName)
        .orElseThrow(IllegalStateException::new);

    return isKeyColumn(newKeyColName);
  }

  private boolean isKeyColumn(final ColumnName fieldName) {
    // until we support structured keys, there will only be a single key column
    // - furthermore, that key column is always prefixed at this point
    // unless it is a join, in which case every other source field is prefixed
    return fieldName.equals(schema.key().get(0).name());
  }

  public SchemaKGroupedStream groupBy(
      final ValueFormat valueFormat,
      final List<Expression> groupByExpressions,
      final Stacker contextStacker
  ) {
    final KeyFormat rekeyedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());

    if (repartitionNotNeeded(groupByExpressions)) {
      return groupByKey(rekeyedKeyFormat, valueFormat, contextStacker);
    }

    final StreamGroupBy<K> source = ExecutionStepFactory.streamGroupBy(
        contextStacker,
        sourceStep,
        Formats.of(rekeyedKeyFormat, valueFormat, SerdeOptions.of()),
        groupByExpressions
    );

    return new SchemaKGroupedStream(
        source,
        resolveSchema(source),
        rekeyedKeyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private SchemaKGroupedStream groupByKey(
      final KeyFormat rekeyedKeyFormat,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker
  ) {
    if (keyFormat.isWindowed()) {
      throw new UnsupportedOperationException("Group by on windowed should always require rekey");
    }
    final StreamGroupByKey step =
        ExecutionStepFactory.streamGroupByKey(
            contextStacker,
            (ExecutionStep) sourceStep,
            Formats.of(rekeyedKeyFormat, valueFormat, SerdeOptions.of())
        );
    return new SchemaKGroupedStream(
        step,
        resolveSchema(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> flatMap(
      final List<FunctionCall> tableFunctions,
      final QueryContext.Stacker contextStacker
  ) {
    final StreamFlatMap<K> step = ExecutionStepFactory.streamFlatMap(
        contextStacker,
        sourceStep,
        tableFunctions
    );
    return new SchemaKStream<>(
        step,
        resolveSchema(step),
        keyFormat,
        ksqlConfig,
        functionRegistry);
  }

  public ExecutionStep<?> getSourceStep() {
    return sourceStep;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public KeyFormat getKeyFormat() {
    return keyFormat;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  LogicalSchema resolveSchema(final ExecutionStep<?> step) {
    return new StepSchemaResolver(ksqlConfig, functionRegistry).resolve(step, schema);
  }

  LogicalSchema resolveSchema(final ExecutionStep<?> step, final SchemaKStream<?> right) {
    return new StepSchemaResolver(ksqlConfig, functionRegistry).resolve(
        step,
        schema,
        right.getSchema()
    );
  }
}
