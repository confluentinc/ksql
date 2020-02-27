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

import io.confluent.ksql.engine.rewrite.StatementRewriteForMagicPseudoTimestamp;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
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
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.JoinWindows;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final String GROUP_BY_COLUMN_SEPARATOR = "|+|";

  public enum Type { SOURCE, PROJECT, FILTER, AGGREGATE, SINK, REKEY, JOIN }

  final KeyFormat keyFormat;
  final KeyField keyField;
  final KsqlConfig ksqlConfig;
  final FunctionRegistry functionRegistry;
  final LogicalSchema schema;
  private final ExecutionStep<KStreamHolder<K>> sourceStep;

  SchemaKStream(
      final ExecutionStep<KStreamHolder<K>> sourceStep,
      final LogicalSchema schema,
      final KeyFormat keyFormat,
      final KeyField keyField,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this.keyFormat = requireNonNull(keyFormat, "keyFormat");
    this.sourceStep = sourceStep;
    this.schema = Objects.requireNonNull(schema, "schema");
    this.keyField = requireNonNull(keyField, "keyField").validateKeyExistsIn(schema);
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
  }

  public SchemaKStream<K> into(
      final String kafkaTopicName,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options,
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
        keyField,
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
        keyField,
        ksqlConfig,
        functionRegistry
    );
  }

  static Expression rewriteTimeComparisonForFilter(final Expression expression) {
    return new StatementRewriteForMagicPseudoTimestamp().rewrite(expression);
  }

  public SchemaKStream<K> select(
      final List<SelectExpression> selectExpressions,
      final QueryContext.Stacker contextStacker,
      final KsqlQueryBuilder ksqlQueryBuilder
  ) {
    final KeyField keyField = findKeyField(selectExpressions);
    final StreamSelect<K> step = ExecutionStepFactory.streamSelect(
        contextStacker,
        sourceStep,
        selectExpressions
    );

    return new SchemaKStream<>(
        step,
        resolveSchema(step),
        keyFormat,
        keyField,
        ksqlConfig,
        functionRegistry
    );
  }

  @SuppressWarnings("deprecation")
  KeyField findKeyField(final List<SelectExpression> selectExpressions) {
    if (!getKeyField().ref().isPresent()) {
      return KeyField.none();
    }

    final ColumnName keyColumnName = getKeyField().ref().get();

    Optional<Column> found = Optional.empty();

    for (final SelectExpression selectExpression : selectExpressions) {
      final ColumnName toName = selectExpression.getAlias();
      final Expression toExpression = selectExpression.getExpression();

      if (toExpression instanceof UnqualifiedColumnReferenceExp) {
        final UnqualifiedColumnReferenceExp nameRef = (UnqualifiedColumnReferenceExp) toExpression;

        if (keyColumnName.equals(nameRef.getReference())) {
          found = Optional.of(Column.legacyKeyFieldColumn(toName, SqlTypes.STRING));
          break;
        }
      }
    }

    final Optional<ColumnName> filtered = found
        // System columns can not be key fields:
        .filter(f -> !SchemaUtil.isSystemColumn(f.name()))
        .map(Column::name);

    return KeyField.of(filtered);
  }


  public SchemaKStream<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final KeyField keyField,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker
  ) {
    final StreamTableJoin<K> step = ExecutionStepFactory.streamTableJoin(
        contextStacker,
        JoinType.LEFT,
        Formats.of(keyFormat, valueFormat, SerdeOption.none()),
        sourceStep,
        schemaKTable.getSourceTableStep()
    );
    return new SchemaKStream<>(
        step,
        resolveSchema(step, schemaKTable),
        keyFormat,
        keyField,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> leftJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
      final QueryContext.Stacker contextStacker) {

    final StreamStreamJoin<K> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.LEFT,
        Formats.of(keyFormat, leftFormat, SerdeOption.none()),
        Formats.of(keyFormat, rightFormat, SerdeOption.none()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinWindows
    );
    return new SchemaKStream<>(
        step,
        resolveSchema(step, otherSchemaKStream),
        keyFormat,
        keyField,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> join(
      final SchemaKTable<K> schemaKTable,
      final KeyField keyField,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker
  ) {
    final StreamTableJoin<K> step = ExecutionStepFactory.streamTableJoin(
        contextStacker,
        JoinType.INNER,
        Formats.of(keyFormat, valueFormat, SerdeOption.none()),
        sourceStep,
        schemaKTable.getSourceTableStep()
    );
    return new SchemaKStream<>(
        step,
        resolveSchema(step, schemaKTable),
        keyFormat,
        keyField,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> join(
      final SchemaKStream<K> otherSchemaKStream,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
      final QueryContext.Stacker contextStacker) {
    final StreamStreamJoin<K> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.INNER,
        Formats.of(keyFormat, leftFormat, SerdeOption.none()),
        Formats.of(keyFormat, rightFormat, SerdeOption.none()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinWindows
    );
    return new SchemaKStream<>(
        step,
        resolveSchema(step, otherSchemaKStream),
        keyFormat,
        keyField,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> outerJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
      final QueryContext.Stacker contextStacker) {
    final StreamStreamJoin<K> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.OUTER,
        Formats.of(keyFormat, leftFormat, SerdeOption.none()),
        Formats.of(keyFormat, rightFormat, SerdeOption.none()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinWindows
    );
    return new SchemaKStream<>(
        step,
        resolveSchema(step, otherSchemaKStream),
        keyFormat,
        keyField,
        ksqlConfig,
        functionRegistry
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKStream<Struct> selectKey(
      final Expression keyExpression,
      final QueryContext.Stacker contextStacker
  ) {
    if (repartitionNotNeeded(keyExpression)) {
      return (SchemaKStream<Struct>) this;
    }

    if (keyFormat.isWindowed()) {
      throw new KsqlException("Implicit repartitioning of windowed sources is not supported. "
          + "See https://github.com/confluentinc/ksql/issues/4385.");
    }

    final StreamSelectKey step = ExecutionStepFactory.streamSelectKey(
        contextStacker,
        sourceStep,
        keyExpression
    );

    return new SchemaKStream<>(
        step,
        resolveSchema(step),
        keyFormat,
        getNewKeyField(keyExpression),
        ksqlConfig,
        functionRegistry
    );
  }

  private KeyField getNewKeyField(final Expression expression) {
    if (!(expression instanceof UnqualifiedColumnReferenceExp)) {
      return KeyField.none();
    }

    final ColumnName columnName = ((UnqualifiedColumnReferenceExp) expression).getReference();
    final KeyField newKeyField = isRowKey(columnName) ? keyField : KeyField.of(columnName);
    return getSchema().isMetaColumn(columnName) ? KeyField.none() : newKeyField;
  }

  protected boolean repartitionNotNeeded(final Expression expression) {
    if (!(expression instanceof UnqualifiedColumnReferenceExp)) {
      return false;
    }

    final ColumnName columnName = ((UnqualifiedColumnReferenceExp) expression).getReference();
    final Optional<Column> existingKey = keyField.resolve(getSchema());

    final Column proposedKey = getSchema()
        .findValueColumn(columnName)
        .orElseThrow(() -> new KsqlException("Invalid identifier for PARTITION BY clause: '"
            + columnName.toString(FormatOptions.noEscape()) + "' Only columns from the "
            + "source schema can be referenced in the PARTITION BY clause."));


    final boolean namesMatch = existingKey
        .map(kf -> kf.name().equals(proposedKey.name()))
        .orElse(false);

    return namesMatch || isRowKey(columnName);
  }

  private boolean isRowKey(final ColumnName fieldName) {
    // until we support structured keys, there will never be any key column other
    // than "ROWKEY" - furthermore, that key column is always prefixed at this point
    // unless it is a join, in which case every other source field is prefixed
    return fieldName.equals(schema.key().get(0).name());
  }

  private static ColumnName fieldNameFromExpression(final Expression expression) {
    if (expression instanceof UnqualifiedColumnReferenceExp) {
      final UnqualifiedColumnReferenceExp nameRef = (UnqualifiedColumnReferenceExp) expression;
      return nameRef.getReference();
    }
    return null;
  }

  private boolean rekeyRequired(final List<Expression> groupByExpressions) {
    if (groupByExpressions.size() != 1) {
      return true;
    }

    final ColumnName groupByField = fieldNameFromExpression(groupByExpressions.get(0));
    if (groupByField == null) {
      return true;
    }

    if (groupByField.equals(SchemaUtil.ROWKEY_NAME)) {
      return false;
    }

    final Optional<Column> keyColumn = getKeyField().resolve(getSchema());
    if (!keyColumn.isPresent()) {
      return true;
    }

    final ColumnName keyFieldName = keyColumn.get().name();
    return !groupByField.equals(keyFieldName);
  }

  public SchemaKGroupedStream groupBy(
      final ValueFormat valueFormat,
      final List<Expression> groupByExpressions,
      final QueryContext.Stacker contextStacker
  ) {
    final boolean rekey = rekeyRequired(groupByExpressions);
    final KeyFormat rekeyedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());
    if (!rekey) {
      return groupByKey(rekeyedKeyFormat, valueFormat, contextStacker);
    }

    final ColumnName aggregateKeyName = groupedKeyNameFor(groupByExpressions);

    final Optional<ColumnName> newKeyCol = getSchema()
        .findValueColumn(aggregateKeyName)
        .map(Column::name);

    final StreamGroupBy<K> source = ExecutionStepFactory.streamGroupBy(
        contextStacker,
        sourceStep,
        Formats.of(rekeyedKeyFormat, valueFormat, SerdeOption.none()),
        groupByExpressions
    );
    return new SchemaKGroupedStream(
        source,
        resolveSchema(source),
        rekeyedKeyFormat,
        KeyField.of(newKeyCol),
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
            Formats.of(rekeyedKeyFormat, valueFormat, SerdeOption.none())
        );
    return new SchemaKGroupedStream(
        step,
        resolveSchema(step),
        keyFormat,
        keyField,
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
        keyField,
        ksqlConfig,
        functionRegistry);
  }

  public ExecutionStep<?> getSourceStep() {
    return sourceStep;
  }

  public KeyField getKeyField() {
    return keyField;
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

  static ColumnName groupedKeyNameFor(final List<Expression> groupByExpressions) {
    if (groupByExpressions.size() == 1
        && groupByExpressions.get(0) instanceof UnqualifiedColumnReferenceExp) {
      return ((UnqualifiedColumnReferenceExp) groupByExpressions.get(0)).getReference();
    }

    // this is safe because if we group by multiple fields the original field
    // will never be in the original schema, so we're necessarily creating a
    // new field
    return ColumnName.of(groupByExpressions.stream()
        .map(Expression::toString)
        .collect(Collectors.joining(GROUP_BY_COLUMN_SEPARATOR)));
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
