/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.ksql.engine.rewrite.StatementRewriteForMagicPseudoTimestamp;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
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
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.planner.plan.PlanBuildContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.InternalFormats;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.SerdeFeaturesFactory;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.serde.none.NoneFormat;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Repartitioning;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  public enum Type { SOURCE, PROJECT, FILTER, AGGREGATE, SINK, REKEY, JOIN, LIMIT }

  final KeyFormat keyFormat;
  final KsqlConfig ksqlConfig;
  final FunctionRegistry functionRegistry;
  final LogicalSchema schema;
  private final ExecutionStep<KStreamHolder<K>> sourceStep;
  private final ExpressionTypeManager typeManager;

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
    this.typeManager = new ExpressionTypeManager(schema, functionRegistry);
  }

  public SchemaKStream<K> into(
      final KsqlTopic topic,
      final QueryContext.Stacker contextStacker,
      final Optional<TimestampColumn> timestampColumn
  ) {
    if (!keyFormat.getWindowInfo().equals(topic.getKeyFormat().getWindowInfo())) {
      throw new IllegalArgumentException("Into can't change windowing");
    }

    final StreamSink<K> step = ExecutionStepFactory.streamSink(
        contextStacker,
        Formats.from(topic),
        sourceStep,
        topic.getKafkaTopicName(),
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
      final PlanBuildContext buildContext,
      final FormatInfo valueFormat
  ) {
    final StreamSelect<K> step = ExecutionStepFactory.streamSelect(
        contextStacker,
        sourceStep,
        keyColumnNames,
        Optional.empty(),
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
      final FormatInfo leftValueFormat,
      final Stacker contextStacker
  ) {
    return join(
      schemaKTable,
      keyColName,
      leftValueFormat,
      contextStacker,
      JoinType.LEFT
    );
  }

  public SchemaKStream<K> leftJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final ColumnName keyColName,
      final WithinExpression withinExpression,
      final FormatInfo leftFormat,
      final FormatInfo rightFormat,
      final Stacker contextStacker
  ) {
    return join(
        otherSchemaKStream,
        keyColName,
        withinExpression,
        leftFormat,
        rightFormat,
        contextStacker,
        JoinType.LEFT
    );
  }

  public SchemaKStream<K> rightJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final ColumnName keyColName,
      final WithinExpression withinExpression,
      final FormatInfo leftFormat,
      final FormatInfo rightFormat,
      final Stacker contextStacker
  ) {
    return join(
        otherSchemaKStream,
        keyColName,
        withinExpression,
        leftFormat,
        rightFormat,
        contextStacker,
        JoinType.RIGHT
    );
  }

  public SchemaKStream<K> innerJoin(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final FormatInfo leftValueFormat,
      final Stacker contextStacker
  ) {
    return join(
      schemaKTable,
      keyColName,
      leftValueFormat,
      contextStacker,
      JoinType.INNER
    );
  }

  public SchemaKStream<K> innerJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final ColumnName keyColName,
      final WithinExpression withinExpression,
      final FormatInfo leftFormat,
      final FormatInfo rightFormat,
      final Stacker contextStacker
  ) {
    return join(
        otherSchemaKStream,
        keyColName,
        withinExpression,
        leftFormat,
        rightFormat,
        contextStacker,
        JoinType.INNER
    );
  }

  public SchemaKStream<K> outerJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final ColumnName keyColName,
      final WithinExpression withinExpression,
      final FormatInfo leftFormat,
      final FormatInfo rightFormat,
      final Stacker contextStacker
  ) {
    return join(
        otherSchemaKStream,
        keyColName,
        withinExpression,
        leftFormat,
        rightFormat,
        contextStacker,
        JoinType.OUTER
    );
  }

  private SchemaKStream<K> join(
      final SchemaKStream<K> otherSchemaKStream,
      final ColumnName keyColName,
      final WithinExpression withinExpression,
      final FormatInfo leftFormat,
      final FormatInfo rightFormat,
      final Stacker contextStacker,
      final JoinType joinType
  ) {
    throwOnJoinKeyFormatsMismatch(otherSchemaKStream);

    final StreamStreamJoin<K> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        joinType,
        keyColName,
        InternalFormats.of(keyFormat, leftFormat),
        InternalFormats.of(keyFormat, rightFormat),
        sourceStep,
        otherSchemaKStream.sourceStep,
        withinExpression.joinWindow(),
        withinExpression.getGrace()
    );

    return new SchemaKStream<>(
        step,
        resolveSchema(step, otherSchemaKStream),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  private SchemaKStream<K> join(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final FormatInfo leftValueFormat,
      final Stacker contextStacker,
      final JoinType joinType
  ) {
    throwOnJoinKeyFormatsMismatch(schemaKTable);

    final StreamTableJoin<K> step = ExecutionStepFactory.streamTableJoin(
        contextStacker,
        joinType,
        keyColName,
        InternalFormats.of(keyFormat, leftValueFormat),
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

  /**
   * @param valueFormat value format used in constructing serdes. Unchanged by this step.
   * @param keyExpression expression for the key being selected
   * @param forceInternalKeyFormat new key format to be used, if present
   * @param contextStacker context for this step
   * @param forceRepartition if true, this step will repartition even if there is no change in
   *                         either key format or value. Used to ensure co-partitioning for
   *                         joins on Schema-Registry-enabled key formats
   * @return result stream: repartitioned if needed or forced, else this stream unchanged
   */
  public SchemaKStream<K> selectKey(
      final FormatInfo valueFormat,
      final List<Expression> keyExpression,
      final Optional<KeyFormat> forceInternalKeyFormat,
      final Stacker contextStacker,
      final boolean forceRepartition
  ) {
    final boolean keyFormatChange = forceInternalKeyFormat.isPresent()
        && !forceInternalKeyFormat.get().equals(keyFormat);

    final boolean repartitionNeeded = repartitionNeeded(keyExpression);
    if (!keyFormatChange && !forceRepartition && !repartitionNeeded) {
      return this;
    }

    if ((repartitionNeeded || !forceRepartition) && keyFormat.isWindowed()) {
      throw new KsqlException(
          "Implicit repartitioning of windowed sources is not supported. "
              + "See https://github.com/confluentinc/ksql/issues/4385."
      );
    }

    final ExecutionStep<KStreamHolder<K>> step = ExecutionStepFactory
        .streamSelectKey(contextStacker, sourceStep, keyExpression);

    final KeyFormat newKeyFormat = forceInternalKeyFormat.orElse(keyFormat);
    return new SchemaKStream<>(
        step,
        resolveSchema(step),
        SerdeFeaturesFactory.sanitizeKeyFormat(
            newKeyFormat,
            toSqlTypes(keyExpression),
            true),
        ksqlConfig,
        functionRegistry
    );
  }

  boolean repartitionNeeded(final List<Expression> expressions) {
    return Repartitioning.repartitionNeeded(schema, expressions);
  }

  public SchemaKGroupedStream groupBy(
      final FormatInfo valueFormat,
      final List<Expression> groupByExpressions,
      final Stacker contextStacker
  ) {
    if (!repartitionNeeded(groupByExpressions)) {
      return groupByKey(keyFormat, valueFormat, contextStacker);
    }

    final KeyFormat rekeyedFormat;
    if (keyFormat.getFormatInfo().getFormat().equals(NoneFormat.NAME)) {
      final FormatInfo defaultFormat = FormatInfo.of(
          ksqlConfig.getString(KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG));
      rekeyedFormat = KeyFormat.nonWindowed(defaultFormat, SerdeFeatures.of());
    } else {
      rekeyedFormat = keyFormat;
    }

    final KeyFormat sanitizedKeyFormat = SerdeFeaturesFactory.sanitizeKeyFormat(
        rekeyedFormat,
        toSqlTypes(groupByExpressions),
        true
    );
    final StreamGroupBy<K> source = ExecutionStepFactory.streamGroupBy(
        contextStacker,
        sourceStep,
        InternalFormats.of(sanitizedKeyFormat, valueFormat),
        groupByExpressions
    );

    return new SchemaKGroupedStream(
        source,
        resolveSchema(source),
        sanitizedKeyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  protected List<SqlType> toSqlTypes(final List<Expression> expressions) {
    return expressions.stream().map(typeManager::getExpressionSqlType).collect(Collectors.toList());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private SchemaKGroupedStream groupByKey(
      final KeyFormat rekeyedKeyFormat,
      final FormatInfo valueFormat,
      final Stacker contextStacker
  ) {
    if (keyFormat.isWindowed()) {
      throw new UnsupportedOperationException("Group by on windowed should always require rekey");
    }
    final StreamGroupByKey step =
        ExecutionStepFactory.streamGroupByKey(
            contextStacker,
            (ExecutionStep) sourceStep,
            InternalFormats.of(rekeyedKeyFormat, valueFormat)
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

  void throwOnJoinKeyFormatsMismatch(final SchemaKStream<?> right) {
    final FormatInfo leftFmt = this.keyFormat.getFormatInfo();
    final FormatInfo rightFmt = right.keyFormat.getFormatInfo();
    if (!leftFmt.equals(rightFmt)) {
      throw new IllegalArgumentException("Key format mismatch in join. "
          + "left: " + leftFmt + ", right: " + rightFmt);
    }

    final SerdeFeatures leftFeats = this.keyFormat.getFeatures();
    final SerdeFeatures rightFeats = right.keyFormat.getFeatures();
    if (!leftFeats.equals(rightFeats)) {
      throw new IllegalArgumentException("Key format features mismatch in join. "
          + "left: " + leftFeats + ", right: " + rightFeats);
    }

    final Optional<WindowType> leftWnd = this.keyFormat.getWindowInfo().map(WindowInfo::getType);
    final Optional<WindowType> rightWnd = right.keyFormat.getWindowInfo().map(WindowInfo::getType);
    if (leftWnd.isPresent() != rightWnd.isPresent()) {
      throw new IllegalArgumentException("Key format windowing mismatch in join. "
          + "left: " + leftWnd + ", right: " + rightWnd);
    }

    final boolean leftIsSession = leftWnd.map(type -> type == WindowType.SESSION).orElse(false);
    final boolean rightIsSession = rightWnd.map(type -> type == WindowType.SESSION).orElse(false);
    if (leftIsSession != rightIsSession) {
      throw new IllegalArgumentException("Key format window type mismatch in join. "
          + "left: " + (leftIsSession ? "Session Windowed" : "Non Session Windowed")
          + ", right: " + (rightIsSession ? "Session Windowed" : "Non Session Windowed")
      );
    }
  }
}
