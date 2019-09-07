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

import static io.confluent.ksql.execution.streams.ExecutionStepFactory.streamSource;
import static io.confluent.ksql.execution.streams.ExecutionStepFactory.streamSourceWindowed;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.codegen.CodeGenRunner;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedNameReference;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepProperties;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.LogicalSchemaWithMetaAndKeyFields;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.StreamSourceBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KeyField.LegacyField;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.streams.StreamsFactories;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.structured.SelectValueMapper.SelectInfo;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.IdentifierUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final FormatOptions FORMAT_OPTIONS =
      FormatOptions.of(IdentifierUtil::needsQuotes);

  public enum Type { SOURCE, PROJECT, FILTER, AGGREGATE, SINK, REKEY, JOIN }

  final KStream<K, GenericRow> kstream;
  final KeyFormat keyFormat;
  final KeySerde<K> keySerde;
  final KeyField keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final Type type;
  final KsqlConfig ksqlConfig;
  final FunctionRegistry functionRegistry;
  final StreamsFactories streamsFactories;
  private final ExecutionStep<KStream<K, GenericRow>> sourceStep;
  private final ExecutionStepProperties sourceProperties;

  private static <K> SchemaKStream<K> forSource(
      final KsqlQueryBuilder builder,
      final KeyFormat keyFormat,
      final KeySerde<K> keySerde,
      final StreamSource<KStream<K, GenericRow>> streamSource,
      final KeyField keyField,
      final KStream<K, GenericRow> kstream) {
    return new SchemaKStream<>(
        kstream,
        streamSource,
        keyFormat,
        keySerde,
        keyField,
        ImmutableList.of(),
        SchemaKStream.Type.SOURCE,
        builder.getKsqlConfig(),
        builder.getFunctionRegistry()
    );
  }

  public static SchemaKStream<?> forSource(
      final KsqlQueryBuilder builder,
      final DataSource<?> dataSource,
      final LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
      final QueryContext.Stacker contextStacker,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset,
      final KeyField keyField
  ) {
    final KsqlTopic topic = dataSource.getKsqlTopic();
    if (topic.getKeyFormat().isWindowed()) {
      final StreamSource<KStream<Windowed<Struct>, GenericRow>> step = streamSourceWindowed(
          contextStacker,
          schemaWithMetaAndKeyFields,
          topic.getKafkaTopicName(),
          Formats.of(topic.getKeyFormat(), topic.getValueFormat(), dataSource.getSerdeOptions()),
          dataSource.getTimestampExtractionPolicy(),
          timestampIndex,
          offsetReset
      );
      return forSource(
          builder,
          topic.getKeyFormat(),
          StreamSourceBuilder.getWindowedKeySerde(builder, step),
          step,
          keyField,
          StreamSourceBuilder.buildWindowed(builder, step));
    } else {
      final StreamSource<KStream<Struct, GenericRow>> step = streamSource(
          contextStacker,
          schemaWithMetaAndKeyFields,
          topic.getKafkaTopicName(),
          Formats.of(topic.getKeyFormat(), topic.getValueFormat(), dataSource.getSerdeOptions()),
          dataSource.getTimestampExtractionPolicy(),
          timestampIndex,
          offsetReset
      );
      return forSource(
          builder,
          topic.getKeyFormat(),
          StreamSourceBuilder.getKeySerde(builder, step),
          step,
          keyField,
          StreamSourceBuilder.buildUnwindowed(builder, step));
    }
  }

  @VisibleForTesting
  SchemaKStream(
      final KStream<K, GenericRow> kstream,
      final ExecutionStep<KStream<K, GenericRow>> sourceStep,
      final KeyFormat keyFormat,
      final KeySerde<K> keySerde,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Type type,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this(
        kstream,
        requireNonNull(sourceStep, "sourceStep"),
        sourceStep.getProperties(),
        keyFormat,
        keySerde,
        keyField,
        sourceSchemaKStreams,
        type,
        ksqlConfig,
        functionRegistry,
        StreamsFactories.create(ksqlConfig)
    );
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumber
  SchemaKStream(
      final KStream<K, GenericRow> kstream,
      final ExecutionStep<KStream<K, GenericRow>> sourceStep,
      final ExecutionStepProperties sourceProperties,
      final KeyFormat keyFormat,
      final KeySerde<K> keySerde,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Type type,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final StreamsFactories streamsFactories
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumber
    this.keyFormat = requireNonNull(keyFormat, "keyFormat");
    this.keySerde = requireNonNull(keySerde, "keySerde");
    this.sourceStep = sourceStep;
    this.sourceProperties = Objects.requireNonNull(sourceProperties, "sourceProperties");
    this.kstream = kstream;
    this.keyField = requireNonNull(keyField, "keyField").validateKeyExistsIn(getSchema());
    this.sourceSchemaKStreams = requireNonNull(sourceSchemaKStreams, "sourceSchemaKStreams");
    this.type = requireNonNull(type, "type");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
    this.streamsFactories = requireNonNull(streamsFactories);
  }

  public SchemaKTable<K> toTable(
      final KeyFormat keyFormat,
      final ValueFormat valueFormat,
      final Serde<GenericRow> valueSerde,
      final QueryContext.Stacker contextStacker
  ) {
    final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        streamsFactories.getMaterializedFactory().create(
            keySerde,
            valueSerde,
            StreamsUtil.buildOpName(contextStacker.getQueryContext()));
    final KTable<K, GenericRow> ktable = kstream
        // 1. mapValues to transform null records into Optional<GenericRow>.EMPTY. We eventually
        //    need to aggregate the KStream to produce the KTable. However the KStream aggregator
        //    filters out records with null keys or values. For tables, a null value for a key
        //    represents that the key was deleted. So we preserve these "tombstone" records by
        //    converting them to a not-null representation.
        .mapValues(Optional::ofNullable)

        // 2. Group by the key, so that we can:
        .groupByKey()

        // 3. Aggregate the KStream into a KTable using a custom aggregator that handles
        // Optional.EMPTY
        .aggregate(
            () -> null,
            (k, value, oldValue) -> value.orElse(null),
            materialized);
    final ExecutionStep<KTable<K, GenericRow>> step = ExecutionStepFactory.streamToTable(
        contextStacker,
        Formats.of(keyFormat, valueFormat, Collections.emptySet()),
        sourceStep
    );
    return new SchemaKTable<>(
        ktable,
        step,
        keyFormat,
        keySerde,
        keyField,
        Collections.singletonList(this),
        type,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> withKeyField(final KeyField resultKeyField) {
    return new SchemaKStream<>(
        kstream,
        sourceStep,
        keyFormat,
        keySerde,
        resultKeyField,
        sourceSchemaKStreams,
        type,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> into(
      final String kafkaTopicName,
      final Serde<GenericRow> topicValueSerDe,
      final LogicalSchema outputSchema,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options,
      final Set<Integer> rowkeyIndexes,
      final QueryContext.Stacker contextStacker
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
    final ExecutionStep<KStream<K, GenericRow>> step = ExecutionStepFactory.streamSink(
        contextStacker,
        outputSchema,
        Formats.of(keyFormat, valueFormat, options),
        sourceStep,
        kafkaTopicName
    );
    return new SchemaKStream<>(
        kstream,
        step,
        keyFormat,
        keySerde,
        keyField,
        Collections.singletonList(this),
        SchemaKStream.Type.SINK,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> filter(
      final Expression filterExpression,
      final QueryContext.Stacker contextStacker,
      final ProcessingLogContext processingLogContext
  ) {
    final SqlPredicate predicate = new SqlPredicate(
        filterExpression,
        getSchema(),
        ksqlConfig,
        functionRegistry,
        processingLogContext.getLoggerFactory().getLogger(
            QueryLoggerUtil.queryLoggerName(
                contextStacker.push(Type.FILTER.name()).getQueryContext())
        )
    );

    final KStream<K, GenericRow> filteredKStream = kstream.filter(predicate.getPredicate());
    final ExecutionStep<KStream<K, GenericRow>> step = ExecutionStepFactory.streamFilter(
        contextStacker,
        sourceStep,
        filterExpression
    );
    return new SchemaKStream<>(
        filteredKStream,
        step,
        keyFormat,
        keySerde,
        keyField,
        Collections.singletonList(this),
        Type.FILTER,
        ksqlConfig,
        functionRegistry
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
    final ExecutionStep<KStream<K, GenericRow>> step = ExecutionStepFactory.streamMapValues(
        contextStacker,
        sourceStep,
        selectExpressions,
        selection.getProjectedSchema()
    );
    return new SchemaKStream<>(
        kstream.mapValues(selection.getSelectValueMapper()),
        step,
        keyFormat,
        keySerde,
        selection.getKey(),
        Collections.singletonList(this),
        Type.PROJECT,
        ksqlConfig,
        functionRegistry
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
      this.key = findKeyField(selectExpressions);
      this.selectValueMapper = SelectValueMapperFactory.create(
          selectExpressions,
          SchemaKStream.this.getSchema(),
          ksqlConfig,
          functionRegistry,
          processingLogger
      );
      this.schema = buildSchema(selectValueMapper);
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
        final SelectValueMapper mapper
    ) {
      final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();

      final List<Field> keyFields = SchemaKStream.this.getSchema().isAliased()
          ? SchemaKStream.this.getSchema().withoutAlias().keyFields()
          : SchemaKStream.this.getSchema().keyFields();

      schemaBuilder.keyFields(keyFields);

      for (final SelectInfo select : mapper.getSelects()) {
        schemaBuilder.valueField(select.getFieldName(), select.getExpressionType());
      }

      return schemaBuilder.build();
    }

    LogicalSchema getProjectedSchema() {
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
      final ValueFormat valueFormat,
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
    final ExecutionStep<KStream<K, GenericRow>> step = ExecutionStepFactory.streamTableJoin(
        contextStacker,
        JoinType.LEFT,
        Formats.of(keyFormat, valueFormat, SerdeOption.none()),
        sourceStep,
        schemaKTable.getSourceTableStep(),
        joinSchema
    );
    return new SchemaKStream<>(
        joinedKStream,
        step,
        keyFormat,
        keySerde,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> leftJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
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
    final ExecutionStep<KStream<K, GenericRow>> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.LEFT,
        Formats.of(keyFormat, leftFormat, SerdeOption.none()),
        Formats.of(keyFormat, rightFormat, SerdeOption.none()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinSchema,
        joinWindows
    );
    return new SchemaKStream<>(
        joinStream,
        step,
        keyFormat,
        keySerde,
        keyField,
        ImmutableList.of(this, otherSchemaKStream),
        Type.JOIN,
        ksqlConfig,
        functionRegistry
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKStream<K> join(
      final SchemaKTable<K> schemaKTable,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final ValueFormat valueFormat,
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
    final ExecutionStep<KStream<K, GenericRow>> step = ExecutionStepFactory.streamTableJoin(
        contextStacker,
        JoinType.INNER,
        Formats.of(keyFormat, valueFormat, SerdeOption.none()),
        sourceStep,
        schemaKTable.getSourceTableStep(),
        joinSchema
    );
    return new SchemaKStream<>(
        joinedKStream,
        step,
        keyFormat,
        keySerde,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> join(
      final SchemaKStream<K> otherSchemaKStream,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
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
    final ExecutionStep<KStream<K, GenericRow>> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.INNER,
        Formats.of(keyFormat, leftFormat, SerdeOption.none()),
        Formats.of(keyFormat, rightFormat, SerdeOption.none()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinSchema,
        joinWindows
    );
    return new SchemaKStream<>(
        joinStream,
        step,
        keyFormat,
        keySerde,
        keyField,
        ImmutableList.of(this, otherSchemaKStream),
        Type.JOIN,
        ksqlConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> outerJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
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
    final ExecutionStep<KStream<K, GenericRow>> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.OUTER,
        Formats.of(keyFormat, leftFormat, SerdeOption.none()),
        Formats.of(keyFormat, rightFormat, SerdeOption.none()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinSchema,
        joinWindows
    );
    return new SchemaKStream<>(
        joinStream,
        step,
        keyFormat,
        keySerde,
        keyField,
        ImmutableList.of(this, otherSchemaKStream),
        Type.JOIN,
        ksqlConfig,
        functionRegistry
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

    final Optional<Field> existingKey = keyField.resolve(getSchema(), ksqlConfig);

    final Field proposedKey = getSchema().findValueField(fieldName)
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
      return (SchemaKStream<Struct>) new SchemaKStream<>(
          kstream,
          sourceStep,
          keyFormat,
          keySerde,
          resultantKeyField,
          sourceSchemaKStreams,
          type,
          ksqlConfig,
          functionRegistry
      );
    }

    final int keyIndexInValue = getSchema().valueFieldIndex(proposedKey.fullName())
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

    final KeyField newKeyField = getSchema().isMetaField(fieldName)
        ? resultantKeyField.withName(Optional.empty())
        : resultantKeyField;

    final KeySerde<Struct> selectKeySerde = keySerde.rebind(StructKeyUtil.ROWKEY_SERIALIZED_SCHEMA);
    final ExecutionStep<KStream<K, GenericRow>> step = ExecutionStepFactory.streamSelectKey(
        contextStacker,
        sourceStep,
        fieldName,
        updateRowKey
    );
    return (SchemaKStream<Struct>) new SchemaKStream(
        keyedKStream,
        step,
        keyFormat,
        selectKeySerde,
        newKeyField,
        Collections.singletonList(this),
        Type.REKEY,
        ksqlConfig,
        functionRegistry
    );
  }

  private boolean usingNewKeyFields() {
    return !ksqlConfig.getBoolean(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD);
  }

  private static boolean isRowKey(final String fieldName) {
    return SchemaUtil.isFieldName(fieldName, SchemaUtil.ROWKEY_NAME);
  }

  private Object extractColumn(final int keyIndexInValue, final GenericRow value) {
    if (value.getColumns().size() != getSchema().valueFields().size()) {
      throw new IllegalStateException("Field count mismatch. "
          + "Schema fields: " + getSchema()
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

    final Optional<Field> keyField = getKeyField().resolve(getSchema(), ksqlConfig);
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
      final ValueFormat valueFormat,
      final Serde<GenericRow> valSerde,
      final List<Expression> groupByExpressions,
      final QueryContext.Stacker contextStacker
  ) {
    final boolean rekey = rekeyRequired(groupByExpressions);
    final KeyFormat rekeyedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());
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
      final ExecutionStep<KGroupedStream<Struct, GenericRow>> step =
          ExecutionStepFactory.streamGroupBy(
              contextStacker,
              sourceStep,
              Formats.of(rekeyedKeyFormat, valueFormat, SerdeOption.none()),
              groupByExpressions
          );
      return new SchemaKGroupedStream(
          kgroupedStream,
          step,
          keyFormat,
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

    final Optional<String> newKeyField = getSchema().findValueField(groupBy.aggregateKeyName)
        .map(Field::name);
    final ExecutionStep<KGroupedStream<Struct, GenericRow>> source =
        ExecutionStepFactory.streamGroupBy(
            contextStacker,
            sourceStep,
            Formats.of(rekeyedKeyFormat, valueFormat, SerdeOption.none()),
            groupByExpressions
        );
    return new SchemaKGroupedStream(
        kgroupedStream,
        source,
        rekeyedKeyFormat,
        groupedKeySerde,
        KeyField.of(newKeyField, Optional.of(legacyKeyField)),
        Collections.singletonList(this),
        ksqlConfig,
        functionRegistry
    );
  }

  ExecutionStep<KStream<K, GenericRow>> getSourceStep() {
    return sourceStep;
  }

  public KeyField getKeyField() {
    return keyField;
  }

  public QueryContext getQueryContext() {
    return sourceProperties.getQueryContext();
  }

  public LogicalSchema getSchema() {
    return sourceProperties.getSchema();
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
        .append(getSchema().toString(FORMAT_OPTIONS))
        .append(" | Logger: ").append(QueryLoggerUtil.queryLoggerName(getQueryContext()))
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

  public KeyFormat getKeyFormat() {
    return keyFormat;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  class GroupBy {

    final String aggregateKeyName;
    final GroupByMapper mapper;

    GroupBy(final List<Expression> expressions) {
      final List<ExpressionMetadata> groupBy = CodeGenRunner.compileExpressions(
          expressions.stream(), "Group By", getSchema(), ksqlConfig, functionRegistry);

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
