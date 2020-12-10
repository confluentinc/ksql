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
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.InternalFormats;
import io.confluent.ksql.serde.InternalFormats.PlaceholderFormats;
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
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.streams.kstream.JoinWindows;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

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
      final FormatInfo leftValueFormat,
      final Stacker contextStacker
  ) {
    throwOnJoinKeyFormatsMismatch(schemaKTable);

    final Function<StreamTableJoin<K>, LogicalSchema> schemaResolver =
        step -> resolveSchema(step, schemaKTable);

    final StreamTableJoin<K> step = buildStepWithFormats(
        formats -> ExecutionStepFactory.streamTableJoin(
            contextStacker,
            JoinType.LEFT,
            keyColName,
            formats,
            sourceStep,
            schemaKTable.getSourceTableStep()
        ),
        schemaResolver,
        keyFormat.getFormatInfo(),
        leftValueFormat
    );

    return new SchemaKStream<>(
        step,
        schemaResolver.apply(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> leftJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final ColumnName keyColName,
      final JoinWindows joinWindows,
      final FormatInfo leftFormat,
      final FormatInfo rightFormat,
      final Stacker contextStacker
  ) {
    throwOnJoinKeyFormatsMismatch(otherSchemaKStream);

    final Function<StreamStreamJoin<K>, LogicalSchema> schemaResolver =
        step -> resolveSchema(step, otherSchemaKStream);

    final StreamStreamJoin<K> step = buildStepWithFormats(
        (leftFormats, rightFormats) -> ExecutionStepFactory.streamStreamJoin(
            contextStacker,
            JoinType.LEFT,
            keyColName,
            leftFormats,
            rightFormats,
            sourceStep,
            otherSchemaKStream.sourceStep,
            joinWindows
        ),
        schemaResolver,
        keyFormat.getFormatInfo(),
        leftFormat,
        rightFormat
    ) ;

    return new SchemaKStream<>(
        step,
        schemaResolver.apply(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> join(
      final SchemaKTable<K> schemaKTable,
      final ColumnName keyColName,
      final FormatInfo leftValueFormat,
      final Stacker contextStacker
  ) {
    throwOnJoinKeyFormatsMismatch(schemaKTable);

    final Function<StreamTableJoin<K>, LogicalSchema> schemaResolver =
        step -> resolveSchema(step, schemaKTable);

    final StreamTableJoin<K> step = buildStepWithFormats(
        formats -> ExecutionStepFactory.streamTableJoin(
            contextStacker,
            JoinType.INNER,
            keyColName,
            formats,
            sourceStep,
            schemaKTable.getSourceTableStep()
        ),
        schemaResolver,
        keyFormat.getFormatInfo(),
        leftValueFormat
    );

    return new SchemaKStream<>(
        step,
        schemaResolver.apply(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> join(
      final SchemaKStream<K> otherSchemaKStream,
      final ColumnName keyColName,
      final JoinWindows joinWindows,
      final FormatInfo leftFormat,
      final FormatInfo rightFormat,
      final Stacker contextStacker
  ) {
    throwOnJoinKeyFormatsMismatch(otherSchemaKStream);

    final Function<StreamStreamJoin<K>, LogicalSchema> schemaResolver =
        step -> resolveSchema(step, otherSchemaKStream);

    final StreamStreamJoin<K> step = buildStepWithFormats(
        (leftFormats, rightFormats) -> ExecutionStepFactory.streamStreamJoin(
            contextStacker,
            JoinType.INNER,
            keyColName,
            leftFormats,
            rightFormats,
            sourceStep,
            otherSchemaKStream.sourceStep,
            joinWindows
        ),
        schemaResolver,
        keyFormat.getFormatInfo(),
        leftFormat,
        rightFormat
    );

    return new SchemaKStream<>(
        step,
        schemaResolver.apply(step),
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> outerJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final ColumnName keyColName,
      final JoinWindows joinWindows,
      final FormatInfo leftFormat,
      final FormatInfo rightFormat,
      final Stacker contextStacker
  ) {
    throwOnJoinKeyFormatsMismatch(otherSchemaKStream);

    final Function<StreamStreamJoin<K>, LogicalSchema> schemaResolver =
        step -> resolveSchema(step, otherSchemaKStream);

    final StreamStreamJoin<K> step = buildStepWithFormats(
        (leftFormats, rightFormats) -> ExecutionStepFactory.streamStreamJoin(
            contextStacker,
            JoinType.OUTER,
            keyColName,
            leftFormats,
            rightFormats,
            sourceStep,
            otherSchemaKStream.sourceStep,
            joinWindows
        ),
        schemaResolver,
        keyFormat.getFormatInfo(),
        leftFormat,
        rightFormat
    );

    return new SchemaKStream<>(
        step,
        schemaResolver.apply(step),
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
      final Expression keyExpression,
      final Optional<FormatInfo> forceInternalKeyFormat,
      final Stacker contextStacker,
      final boolean forceRepartition
  ) {
    final boolean keyFormatChange = forceInternalKeyFormat.isPresent()
        && !forceInternalKeyFormat.get().equals(keyFormat.getFormatInfo());

    final boolean repartitionNeeded = repartitionNeeded(ImmutableList.of(keyExpression));
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

    final LogicalSchema newSchema = resolveSchema(step);

    final KeyFormat newKeyFormat = forceInternalKeyFormat
        .map(newFmt -> KeyFormat.of(
            newFmt,
            SerdeFeaturesFactory.buildInternalKeyFeatures(newSchema, FormatFactory.of(newFmt)),
            keyFormat.getWindowInfo()))
        .orElse(keyFormat);

    return new SchemaKStream<>(
        step,
        newSchema,
        newKeyFormat,
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
      return groupByKey(keyFormat.getFormatInfo(), valueFormat, contextStacker);
    }

    final FormatInfo keyFmtInfo = keyFormat.getFormatInfo().getFormat().equals(NoneFormat.NAME)
        ? FormatInfo.of(ksqlConfig.getString(KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG))
        : keyFormat.getFormatInfo();

    final KeyFormat rekeyedKeyFormat = KeyFormat
        .nonWindowed(keyFmtInfo, SerdeFeatures.of());

    final StreamGroupBy<K> source = buildStepWithFormats(
        formats -> ExecutionStepFactory.streamGroupBy(
            contextStacker,
            sourceStep,
            formats,
            groupByExpressions
        ),
        rekeyedKeyFormat.getFormatInfo(),
        valueFormat
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
      final FormatInfo rekeyedKeyFormat,
      final FormatInfo valueFormat,
      final QueryContext.Stacker contextStacker
  ) {
    if (keyFormat.isWindowed()) {
      throw new UnsupportedOperationException("Group by on windowed should always require rekey");
    }
    final StreamGroupByKey step = buildStepWithFormats(
        formats -> ExecutionStepFactory.streamGroupByKey(
            contextStacker,
            (ExecutionStep) sourceStep,
            formats
        ),
        rekeyedKeyFormat,
        valueFormat
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

  protected <E extends ExecutionStep<?>> E buildStepWithFormats(
      final Function<Formats, E> stepSupplier,
      final FormatInfo keyFormat,
      final FormatInfo valueFormat
  ) {
    return buildStepWithFormats(
        stepSupplier,
        this::resolveSchema,
        keyFormat,
        valueFormat
    );
  }

  protected <E extends ExecutionStep<?>> E buildStepWithFormats(
      final Function<Formats, E> stepSupplier,
      final Function<E, LogicalSchema> schemaResolver,
      final FormatInfo keyFormat,
      final FormatInfo valueFormat
  ) {
    final E placeholderStep = stepSupplier.apply(PlaceholderFormats.of());
    final LogicalSchema schema = schemaResolver.apply(placeholderStep);
    return stepSupplier.apply(InternalFormats.of(schema, keyFormat, valueFormat));
  }

  protected <E extends ExecutionStep<?>> E buildStepWithFormats(
      final BiFunction<Formats, Formats, E> stepSupplier,
      final Function<E, LogicalSchema> schemaResolver,
      final FormatInfo keyFormat,
      final FormatInfo leftValueFormat,
      final FormatInfo rightValueFormat
  ) {
    final E placeholderStep = stepSupplier.apply(
        PlaceholderFormats.of(),
        PlaceholderFormats.of()
    );
    final LogicalSchema schema = schemaResolver.apply(placeholderStep);
    return stepSupplier.apply(
        InternalFormats.of(schema, keyFormat, leftValueFormat),
        InternalFormats.of(schema, keyFormat, rightValueFormat)
    );
  }
}
