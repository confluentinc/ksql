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
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.codegen.CodeGenRunner;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KeyField.LegacyField;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.streams.StreamsFactories;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ParserUtil;
import io.confluent.ksql.util.QueryLoggerUtil;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SelectExpression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final FormatOptions FORMAT_OPTIONS =
      FormatOptions.of(ParserUtil::isReservedIdentifier);

  public enum Type { SOURCE, PROJECT, FILTER, AGGREGATE, SINK, REKEY, JOIN }

  final KStream<K, GenericRow> kstream;
  final LogicalSchema schema;
  final KeySerde<K> keySerde;
  final KeyField keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final Type type;
  final KsqlConfig ksqlConfig;
  final FunctionRegistry functionRegistry;
  final StreamsFactories streamsFactories;
  private final QueryContext queryContext;

  public SchemaKStream(
      final KStream<K, GenericRow> kstream,
      final LogicalSchema schema,
      final KeySerde<K> keySerde,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Type type,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final QueryContext queryContext
  ) {
    this(
        kstream, schema,
        keySerde,
        keyField,
        sourceSchemaKStreams,
        type,
        ksqlConfig,
        functionRegistry,
        StreamsFactories.create(ksqlConfig),
        queryContext);
  }

  SchemaKStream(
      final KStream<K, GenericRow> kstream,
      final LogicalSchema schema,
      final KeySerde<K> keySerde,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Type type,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final StreamsFactories streamsFactories,
      final QueryContext queryContext
  ) {
    this.schema = requireNonNull(schema, "schema");
    this.keySerde = requireNonNull(keySerde, "keySerde");
    this.kstream = kstream;
    this.keyField = requireNonNull(keyField, "keyField")
        .validateKeyExistsIn(schema);
    this.sourceSchemaKStreams = requireNonNull(sourceSchemaKStreams, "sourceSchemaKStreams");
    this.type = requireNonNull(type, "type");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
    this.streamsFactories = requireNonNull(streamsFactories);
    this.queryContext = requireNonNull(queryContext);
  }

  public SchemaKStream into(
      final String kafkaTopicName,
      final Serde<GenericRow> topicValueSerDe,
      final Set<Integer> rowkeyIndexes
  ) {
    kstream
        .mapValues(row -> {
          if (row == null) {
            return null;
          }
          final List<Object> columns = new ArrayList<>();
          for (int i = 0; i < row.getColumns().size(); i++) {
            if (!rowkeyIndexes.contains(i)) {
              columns.add(row.getColumns().get(i));
            }
          }
          return new GenericRow(columns);
        }).to(kafkaTopicName, Produced.with(keySerde, topicValueSerDe));
    return this;
  }

  public SchemaKStream<K> filter(
      final Expression filterExpression,
      final QueryContext.Stacker contextStacker,
      final ProcessingLogContext processingLogContext
  ) {
    final SqlPredicate predicate = new SqlPredicate(
        filterExpression,
        schema,
        ksqlConfig,
        functionRegistry,
        processingLogContext.getLoggerFactory().getLogger(
            QueryLoggerUtil.queryLoggerName(
                contextStacker.push(Type.FILTER.name()).getQueryContext())
        )
    );

    final KStream<K, GenericRow> filteredKStream = kstream.filter(predicate.getPredicate());
    return new SchemaKStream<>(
        filteredKStream,
        schema,
        keySerde,
        keyField,
        Collections.singletonList(this),
        Type.FILTER,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  public SchemaKStream<K> select(
      final List<SelectExpression> selectExpressions,
      final QueryContext.Stacker contextStacker,
      final ProcessingLogContext processingLogContext) {
    final Selection selection = new Selection(
        selectExpressions,
        processingLogContext.getLoggerFactory().getLogger(
            QueryLoggerUtil.queryLoggerName(
                contextStacker.push(Type.PROJECT.name()).getQueryContext()))
    );
    return new SchemaKStream<>(
        kstream.mapValues(selection.getSelectValueMapper()),
        selection.getProjectedSchema(),
        keySerde,
        selection.getKey(),
        Collections.singletonList(this),
        Type.PROJECT,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  class Selection {

    private final LogicalSchema schema;
    private final KeyField key;
    private final SelectValueMapper selectValueMapper;

    Selection(
        final List<SelectExpression> selectExpressions,
        final ProcessingLogger processingLogger
    ) {
      key = findKeyField(selectExpressions);
      final List<ExpressionMetadata> expressionEvaluators = buildExpressions(selectExpressions);
      schema = buildSchema(selectExpressions, expressionEvaluators);
      final List<String> selectFieldNames = selectExpressions.stream()
          .map(SelectExpression::getName)
          .collect(Collectors.toList());
      selectValueMapper = new SelectValueMapper(
          selectFieldNames,
          expressionEvaluators,
          processingLogger);
    }

    private KeyField findKeyField(final List<SelectExpression> selectExpressions) {
      return KeyField.of(findNewKeyField(selectExpressions), findLegacyKeyField(selectExpressions));
    }

    private Optional<String> findNewKeyField(final List<SelectExpression> selectExpressions) {
      if (!getKeyField().name().isPresent()) {
        return Optional.empty();
      }

      final String fullFieldName = getKeyField().name().get();
      final Optional<String> fieldAlias = SchemaUtil.getFieldNameAlias(fullFieldName);
      final String fieldNameWithNoAlias = SchemaUtil.getFieldNameWithNoAlias(fullFieldName);

      final Field keyField = Field.of(fieldAlias, fieldNameWithNoAlias, SqlTypes.STRING);

      return doFindKeyField(selectExpressions, keyField)
          .map(Field::fullName);
    }

    private Optional<LegacyField> findLegacyKeyField(
        final List<SelectExpression> selectExpressions
    ) {
      if (!getKeyField().legacy().isPresent()) {
        return Optional.empty();
      }

      final LegacyField keyField = getKeyField().legacy().get();
      if (keyField.isNotInSchema()) {
        // The key "field" isn't an actual field in the schema
        return Optional.of(keyField);
      }

      return doFindKeyField(selectExpressions, Field.of(keyField.name(), keyField.type()))
          .map(field -> LegacyField.of(field.fullName(), field.type()));
    }

    private Optional<Field> doFindKeyField(
        final List<SelectExpression> selectExpressions,
        final Field keyField
    ) {
      Optional<Field> found = Optional.empty();

      for (int i = 0; i < selectExpressions.size(); i++) {
        final String toName = selectExpressions.get(i).getName();
        final Expression toExpression = selectExpressions.get(i).getExpression();

        /*
         * Sometimes a column reference is a DereferenceExpression, and sometimes its
         * a QualifiedNameReference. We have an issue
         * (https://github.com/confluentinc/ksql/issues/1695)
         * to track cleaning this up and using DereferenceExpression for all column references.
         * Until then, we have to check for both here.
         */
        if (toExpression instanceof DereferenceExpression) {
          final DereferenceExpression deRef
              = (DereferenceExpression) toExpression;

          if (SchemaUtil.isFieldName(deRef.toString(), keyField.fullName())) {
            found = Optional.of(Field.of(toName, keyField.type()));
            break;
          }
        } else if (toExpression instanceof QualifiedNameReference) {
          final QualifiedNameReference nameRef
              = (QualifiedNameReference) toExpression;

          if (SchemaUtil.isFieldName(nameRef.getName().getSuffix(), keyField.fullName())) {
            found = Optional.of(Field.of(toName, keyField.type()));
            break;
          }
        }
      }

      return found
          .filter(f -> !SchemaUtil.isFieldName(f.name(), SchemaUtil.ROWTIME_NAME))
          .filter(f -> !SchemaUtil.isFieldName(f.name(), SchemaUtil.ROWKEY_NAME));
    }

    private LogicalSchema buildSchema(
        final List<SelectExpression> selectExpressions,
        final List<ExpressionMetadata> expressionEvaluators
    ) {
      final SchemaBuilder valueSchema = SchemaBuilder.struct();
      IntStream.range(0, selectExpressions.size()).forEach(
          i -> valueSchema.field(
              selectExpressions.get(i).getName(),
              expressionEvaluators.get(i).getExpressionType()));

      final ConnectSchema keySchema = SchemaKStream.this.schema.isAliased()
          ? SchemaKStream.this.schema.withoutAlias().keySchema()
          : SchemaKStream.this.schema.keySchema();

      return LogicalSchema.of(keySchema, valueSchema.build());
    }

    List<ExpressionMetadata> buildExpressions(final List<SelectExpression> selectExpressions
    ) {
      final Stream<Expression> expressions = selectExpressions.stream()
          .map(SelectExpression::getExpression);

      return CodeGenRunner.compileExpressions(
          expressions, "Select", SchemaKStream.this.getSchema(), ksqlConfig, functionRegistry);
    }

    public LogicalSchema getProjectedSchema() {
      return schema;
    }

    public KeyField getKey() {
      return key;
    }

    SelectValueMapper getSelectValueMapper() {
      return selectValueMapper;
    }
  }

  @SuppressWarnings("unchecked")
  public SchemaKStream<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final Serde<GenericRow> leftValueSerDe,
      final QueryContext.Stacker contextStacker
  ) {

    final KStream<K, GenericRow> joinedKStream =
        kstream.leftJoin(
            schemaKTable.getKtable(),
            new KsqlValueJoiner(this.getSchema(), schemaKTable.getSchema()),
            streamsFactories.getJoinedFactory().create(
                keySerde,
                leftValueSerDe,
                null,
                StreamsUtil.buildOpName(contextStacker.getQueryContext()))
        );

    return new SchemaKStream<>(
        joinedKStream,
        joinSchema,
        keySerde,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  public SchemaKStream<K> leftJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final Serde<GenericRow> leftSerde,
      final Serde<GenericRow> rightSerde,
      final QueryContext.Stacker contextStacker) {

    final KStream<K, GenericRow> joinStream =
        kstream
            .leftJoin(
                otherSchemaKStream.kstream,
                new KsqlValueJoiner(this.getSchema(), otherSchemaKStream.getSchema()),
                joinWindows,
                streamsFactories.getJoinedFactory().create(
                    keySerde,
                    leftSerde,
                    rightSerde,
                    StreamsUtil.buildOpName(contextStacker.getQueryContext()))
            );

    return new SchemaKStream<>(
        joinStream,
        joinSchema,
        keySerde,
        keyField,
        ImmutableList.of(this, otherSchemaKStream),
        Type.JOIN,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKStream<K> join(
      final SchemaKTable<K> schemaKTable,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final Serde<GenericRow> joinSerDe,
      final QueryContext.Stacker contextStacker
  ) {
    final KStream<K, GenericRow> joinedKStream =
        kstream.join(
            schemaKTable.getKtable(),
            new KsqlValueJoiner(getSchema(), schemaKTable.getSchema()),
            streamsFactories.getJoinedFactory().create(
                keySerde,
                joinSerDe,
                null,
                StreamsUtil.buildOpName(contextStacker.getQueryContext()))
        );

    return new SchemaKStream<>(
        joinedKStream,
        joinSchema,
        keySerde,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  public SchemaKStream<K> join(
      final SchemaKStream<K> otherSchemaKStream,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final Serde<GenericRow> leftSerde,
      final Serde<GenericRow> rightSerde,
      final QueryContext.Stacker contextStacker) {
    final KStream<K, GenericRow> joinStream =
        kstream
            .join(
                otherSchemaKStream.kstream,
                new KsqlValueJoiner(this.getSchema(), otherSchemaKStream.getSchema()),
                joinWindows,
                streamsFactories.getJoinedFactory().create(
                    keySerde,
                    leftSerde,
                    rightSerde,
                    StreamsUtil.buildOpName(contextStacker.getQueryContext()))
            );

    return new SchemaKStream<>(
        joinStream,
        joinSchema,
        keySerde,
        keyField,
        ImmutableList.of(this, otherSchemaKStream),
        Type.JOIN,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  public SchemaKStream<K> outerJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final Serde<GenericRow> leftSerde,
      final Serde<GenericRow> rightSerde,
      final QueryContext.Stacker contextStacker) {
    final KStream<K, GenericRow> joinStream = kstream
        .outerJoin(
            otherSchemaKStream.kstream,
            new KsqlValueJoiner(this.getSchema(), otherSchemaKStream.getSchema()),
            joinWindows,
            streamsFactories.getJoinedFactory().create(
                keySerde,
                leftSerde,
                rightSerde,
                StreamsUtil.buildOpName(contextStacker.getQueryContext()))
        );

    return new SchemaKStream<>(
        joinStream,
        joinSchema,
        keySerde,
        keyField,
        ImmutableList.of(this, otherSchemaKStream),
        Type.JOIN,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKStream<Struct> selectKey(
      final String fieldName,
      final boolean updateRowKey,
      final QueryContext.Stacker contextStacker
  ) {
    if (keySerde.isWindowed()) {
      throw new UnsupportedOperationException("Can not selectKey of windowed stream");
    }

    final Optional<Field> existingKey = keyField.resolve(schema, ksqlConfig);

    final Field proposedKey = schema.findValueField(fieldName)
        .orElseThrow(IllegalArgumentException::new);

    final LegacyField proposedLegacy = LegacyField.of(proposedKey.fullName(), proposedKey.type());

    final KeyField resultantKeyField = isRowKey(fieldName)
            ? keyField.withLegacy(proposedLegacy)
            : KeyField.of(fieldName, proposedLegacy);

    final boolean namesMatch = existingKey
        .map(kf -> SchemaUtil.isFieldName(proposedKey.fullName(), kf.fullName()))
        .orElse(false);

    // Note: Prior to v5.3 a selectKey(ROWKEY) would result in a repartition.
    // To maintain compatibility, old queries, started prior to v5.3, must have repartition step.
    // So we only handle rowkey for new queries:
    final boolean treatAsRowKey = usingNewKeyFields() && isRowKey(proposedKey.name());

    if (namesMatch || treatAsRowKey) {
      return new SchemaKStream<>(
          kstream,
          schema,
          (KeySerde) keySerde,
          resultantKeyField,
          sourceSchemaKStreams,
          type,
          ksqlConfig,
          functionRegistry,
          queryContext
      );
    }

    final int keyIndexInValue = schema.valueFieldIndex(proposedKey.fullName())
        .orElseThrow(IllegalStateException::new);

    final KStream keyedKStream = kstream
        .filter((key, value) -> value != null && extractColumn(keyIndexInValue, value) != null)
        .selectKey((key, value) ->
            StructKeyUtil.asStructKey(extractColumn(keyIndexInValue, value).toString()))
        .mapValues((key, row) -> {
          if (updateRowKey) {
            final Object rowKey = key.get(key.schema().fields().get(0));
            row.getColumns().set(SchemaUtil.ROWKEY_INDEX, rowKey);
          }
          return row;
        });

    final KeyField newKeyField = schema.isMetaField(fieldName)
        ? resultantKeyField.withName(Optional.empty())
        : resultantKeyField;

    final KeySerde<Struct> selectKeySerde = keySerde.rebind(StructKeyUtil.ROWKEY_SERIALIZED_SCHEMA);

    return new SchemaKStream<>(
        keyedKStream,
        schema,
        selectKeySerde,
        newKeyField,
        Collections.singletonList(this),
        Type.REKEY,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );
  }

  private boolean usingNewKeyFields() {
    return !ksqlConfig.getBoolean(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD);
  }

  private static boolean isRowKey(final String fieldName) {
    return SchemaUtil.isFieldName(fieldName, SchemaUtil.ROWKEY_NAME);
  }

  private Object extractColumn(final int keyIndexInValue, final GenericRow value) {
    if (value.getColumns().size() != schema.valueFields().size()) {
      throw new IllegalStateException("Field count mismatch. "
          + "Schema fields: " + schema
          + ", row:" + value);
    }

    return value
        .getColumns()
        .get(keyIndexInValue);
  }

  private static String fieldNameFromExpression(final Expression expression) {
    if (expression instanceof DereferenceExpression) {
      final DereferenceExpression dereferenceExpression =
          (DereferenceExpression) expression;
      return dereferenceExpression.getFieldName();
    } else if (expression instanceof QualifiedNameReference) {
      final QualifiedNameReference qualifiedNameReference = (QualifiedNameReference) expression;
      return qualifiedNameReference.getName().toString();
    }
    return null;
  }

  private boolean rekeyRequired(final List<Expression> groupByExpressions) {
    if (groupByExpressions.size() != 1) {
      return true;
    }

    final Optional<Field> keyField = getKeyField().resolve(schema, ksqlConfig);
    if (!keyField.isPresent()) {
      return true;
    }

    final String groupByField = fieldNameFromExpression(groupByExpressions.get(0));
    if (groupByField == null) {
      return true;
    }

    final String keyFieldName = keyField.get().name();
    return !groupByField.equals(keyFieldName);
  }

  @SuppressWarnings("unchecked")
  public SchemaKGroupedStream groupBy(
      final Serde<GenericRow> valSerde,
      final List<Expression> groupByExpressions,
      final QueryContext.Stacker contextStacker
  ) {
    final boolean rekey = rekeyRequired(groupByExpressions);
    if (!rekey) {
      final Grouped<K, GenericRow> grouped = streamsFactories.getGroupedFactory()
          .create(
              StreamsUtil.buildOpName(contextStacker.getQueryContext()),
              keySerde,
              valSerde
          );

      final KGroupedStream kgroupedStream = kstream.groupByKey(grouped);

      if (keySerde.isWindowed()) {
        throw new UnsupportedOperationException("Group by on windowed should always require rekey");
      }

      final KeySerde<Struct> structKeySerde = (KeySerde) keySerde;

      return new SchemaKGroupedStream(
          kgroupedStream,
          schema,
          structKeySerde,
          keyField,
          Collections.singletonList(this),
          ksqlConfig,
          functionRegistry
      );
    }

    final GroupBy groupBy = new GroupBy(groupByExpressions);

    final KeySerde<Struct> groupedKeySerde = keySerde
        .rebind(StructKeyUtil.ROWKEY_SERIALIZED_SCHEMA);

    final Grouped<Struct, GenericRow> grouped = streamsFactories.getGroupedFactory()
        .create(
            StreamsUtil.buildOpName(contextStacker.getQueryContext()),
            groupedKeySerde,
            valSerde
        );

    final KGroupedStream kgroupedStream = kstream
        .filter((key, value) -> value != null)
        .groupBy(groupBy.mapper, grouped);

    final LegacyField legacyKeyField = LegacyField
        .notInSchema(groupBy.aggregateKeyName, SqlTypes.STRING);

    final Optional<String> newKeyField = schema.findValueField(groupBy.aggregateKeyName)
        .map(Field::name);

    return new SchemaKGroupedStream(
        kgroupedStream,
        schema,
        groupedKeySerde,
        KeyField.of(newKeyField, Optional.of(legacyKeyField)),
        Collections.singletonList(this),
        ksqlConfig,
        functionRegistry
    );
  }

  public KeyField getKeyField() {
    return keyField;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public KeySerde<K> getKeySerde() {
    return keySerde;
  }

  public KStream<K, GenericRow> getKstream() {
    return kstream;
  }

  public List<SchemaKStream> getSourceSchemaKStreams() {
    return sourceSchemaKStreams;
  }

  public String getExecutionPlan(final String indent) {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(indent)
        .append(" > [ ")
        .append(type).append(" ] | Schema: ")
        .append(schema.toString(FORMAT_OPTIONS))
        .append(" | Logger: ").append(QueryLoggerUtil.queryLoggerName(queryContext))
        .append("\n");
    for (final SchemaKStream schemaKStream : sourceSchemaKStreams) {
      stringBuilder
          .append("\t")
          .append(indent)
          .append(schemaKStream.getExecutionPlan(indent + "\t"));
    }
    return stringBuilder.toString();
  }

  public Type getType() {
    return type;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  class GroupBy {

    final String aggregateKeyName;
    final GroupByMapper mapper;

    GroupBy(final List<Expression> expressions) {
      final List<ExpressionMetadata> groupBy = CodeGenRunner.compileExpressions(
          expressions.stream(), "Group By", schema, ksqlConfig, functionRegistry);

      this.mapper = new GroupByMapper(groupBy);
      this.aggregateKeyName = GroupByMapper.keyNameFor(expressions);
    }
  }

  protected static class KsqlValueJoiner
      implements ValueJoiner<GenericRow, GenericRow, GenericRow> {

    private final LogicalSchema leftSchema;
    private final LogicalSchema rightSchema;

    KsqlValueJoiner(final LogicalSchema leftSchema, final LogicalSchema rightSchema) {
      this.leftSchema = Objects.requireNonNull(leftSchema, "leftSchema");
      this.rightSchema = Objects.requireNonNull(rightSchema, "rightSchema");
    }

    @Override
    public GenericRow apply(final GenericRow left, final GenericRow right) {
      final List<Object> columns = new ArrayList<>();
      if (left != null) {
        columns.addAll(left.getColumns());
      } else {
        fillWithNulls(columns, leftSchema.valueFields().size());
      }

      if (right != null) {
        columns.addAll(right.getColumns());
      } else {
        fillWithNulls(columns, rightSchema.valueFields().size());
      }

      return new GenericRow(columns);
    }

    private static void fillWithNulls(final List<Object> columns, final int numToFill) {
      for (int i = 0; i < numToFill; ++i) {
        columns.add(null);
      }
    }
  }
}
