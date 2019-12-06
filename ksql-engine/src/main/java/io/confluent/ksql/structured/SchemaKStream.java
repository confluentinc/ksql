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

import io.confluent.ksql.engine.rewrite.StatementRewriteForRowtime;
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
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
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
      final QueryContext.Stacker contextStacker
  ) {
    final StreamSink<K> step = ExecutionStepFactory.streamSink(
        contextStacker,
        Formats.of(keyFormat, valueFormat, options),
        sourceStep,
        kafkaTopicName
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
    return new StatementRewriteForRowtime()
        .rewriteForRowtime(expression);
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

    final ColumnRef keyColumnRef = getKeyField().ref().get();

    Optional<Column> found = Optional.empty();

    for (SelectExpression selectExpression : selectExpressions) {
      final ColumnName toName = selectExpression.getAlias();
      final Expression toExpression = selectExpression.getExpression();

      if (toExpression instanceof ColumnReferenceExp) {
        final ColumnReferenceExp nameRef = (ColumnReferenceExp) toExpression;

        if (keyColumnRef.equals(nameRef.getReference())) {
          found = Optional.of(Column.legacyKeyFieldColumn(toName, SqlTypes.STRING));
          break;
        }
      }
    }

    final Optional<ColumnRef> filtered = found
        .filter(f -> !SchemaUtil.isFieldName(f.name().name(), SchemaUtil.ROWTIME_NAME.name()))
        .filter(f -> !SchemaUtil.isFieldName(f.name().name(), SchemaUtil.ROWKEY_NAME.name()))
        .map(Column::ref);

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
    if (keyFormat.isWindowed()) {
      throw new UnsupportedOperationException("Can not selectKey of windowed stream");
    }

    if (!needsRepartition(keyExpression)) {
      return (SchemaKStream<Struct>) this;
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
    if (!(expression instanceof ColumnReferenceExp)) {
      return KeyField.none();
    }

    final ColumnRef columnRef = ((ColumnReferenceExp) expression).getReference();
    final KeyField newKeyField = isRowKey(columnRef) ? keyField : KeyField.of(columnRef);
    return getSchema().isMetaColumn(columnRef.name()) ? KeyField.none() : newKeyField;
  }

  private boolean needsRepartition(final Expression expression) {
    if (!(expression instanceof ColumnReferenceExp)) {
      return true;
    }

    final ColumnRef columnRef = ((ColumnReferenceExp) expression).getReference();
    final Optional<Column> existingKey = keyField.resolve(getSchema());

    final Column proposedKey = getSchema()
        .findValueColumn(columnRef)
        .orElseThrow(() -> new KsqlException("Invalid identifier for PARTITION BY clause: '"
            + columnRef.name().toString(FormatOptions.noEscape()) + "' Only columns from the "
            + "source schema can be referenced in the PARTITION BY clause."));


    final boolean namesMatch = existingKey
        .map(kf -> kf.ref().equals(proposedKey.ref()))
        .orElse(false);

    return !namesMatch && !isRowKey(columnRef);
  }

  private static boolean isRowKey(final ColumnRef fieldName) {
    return fieldName.name().equals(SchemaUtil.ROWKEY_NAME);
  }

  private static ColumnName fieldNameFromExpression(final Expression expression) {
    if (expression instanceof ColumnReferenceExp) {
      final ColumnReferenceExp nameRef = (ColumnReferenceExp) expression;
      return nameRef.getReference().name();
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

    final ColumnRef aggregateKeyName = groupedKeyNameFor(groupByExpressions);

    final Optional<ColumnRef> newKeyCol = getSchema()
        .findValueColumn(aggregateKeyName)
        .map(Column::ref);

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

  @SuppressWarnings("unchecked")
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

  static ColumnRef groupedKeyNameFor(final List<Expression> groupByExpressions) {
    if (groupByExpressions.size() == 1 && groupByExpressions.get(0) instanceof ColumnReferenceExp) {
      return ((ColumnReferenceExp) groupByExpressions.get(0)).getReference();
    }

    // this is safe because if we group by multiple fields the original field
    // will never be in the original schema, so we're necessarily creating a
    // new field
    return ColumnRef.withoutSource(
        ColumnName.of(groupByExpressions.stream()
            .map(Expression::toString)
            .collect(Collectors.joining(GROUP_BY_COLUMN_SEPARATOR))));
  }

  LogicalSchema resolveSchema(final ExecutionStep<?> step) {
    return new StepSchemaResolver(ksqlConfig, functionRegistry).resolve(step, schema);
  }

  LogicalSchema resolveSchema(final ExecutionStep<?> step, SchemaKStream right) {
    return new StepSchemaResolver(ksqlConfig, functionRegistry).resolve(
        step,
        schema,
        right.getSchema()
    );
  }
}
