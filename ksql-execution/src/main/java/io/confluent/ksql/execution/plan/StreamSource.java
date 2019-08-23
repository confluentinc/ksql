/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.plan;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KsqlConsumed;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;

@Immutable
public class StreamSource<K> implements ExecutionStep<KStream<K, GenericRow>> {
  private final ExecutionStepProperties properties;
  private final String topicName;
  private final Formats formats;
  private final TimestampExtractionPolicy timestampPolicy;
  private final int timestampIndex;
  private final Optional<AutoOffsetReset> offsetReset;
  private final LogicalSchema sourceSchema;
  // These fields are not serialized. They are computed when deserialized
  private final Functions<K> functions;

  @VisibleForTesting
  StreamSource(
      final ExecutionStepProperties properties,
      final String topicName,
      final Formats formats,
      final TimestampExtractionPolicy timestampPolicy,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset,
      final LogicalSchema sourceSchema,
      final Functions<K> functions) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.timestampPolicy = Objects.requireNonNull(timestampPolicy, "timestampPolicy");
    this.timestampIndex = timestampIndex;
    this.offsetReset = Objects.requireNonNull(offsetReset, "offsetReset");
    this.sourceSchema = Objects.requireNonNull(sourceSchema, "sourceSchema");
    this.functions = Objects.requireNonNull(functions, "function");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamSource that = (StreamSource) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(formats, that.formats)
        && Objects.equals(timestampPolicy, that.timestampPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        properties,
        topicName,
        formats,
        timestampPolicy
    );
  }

  @Override
  public KStream<K, GenericRow> build(final KsqlQueryBuilder queryBuilder) {
    // this hack will go away soon. it's just glue until we don't need keySerde in
    // SchemaKStream/SchemaKTable anymore
    return buildWithSerde(queryBuilder).getRight();
  }

  private Consumed<K, GenericRow> buildSourceConsumed(
      final Serde<K> keySerde,
      final Serde<GenericRow> valueSerde) {
    final TimestampExtractor timestampExtractor = timestampPolicy.create(timestampIndex);
    final Consumed<K, GenericRow> consumed = functions.consumedFactory
        .apply(keySerde, valueSerde)
        .withTimestampExtractor(timestampExtractor);
    if (offsetReset.isPresent()) {
      return consumed.withOffsetResetPolicy(offsetReset.get());
    }
    return consumed;
  }

  private KStream<K, GenericRow> buildKStream(
      final KsqlQueryBuilder queryBuilder,
      final Consumed<K, GenericRow> consumed,
      final ValueMapperWithKey<K, GenericRow, GenericRow> mapper) {
    return queryBuilder.getStreamsBuilder()
        // 1. Create a KStream on the changelog topic.
        .stream(topicName, consumed)
        // 2. mapValues to add the ROWKEY column
        .mapValues(mapper)
        // 3. transformValues to add the ROWTIME column. transformValues is required to access the
        //    streams ProcessorContext which has the timestamp for the record.
        .transformValues(functions.timestampTransformer);
  }

  public Pair<KeySerde<K>, KStream<K, GenericRow>> buildWithSerde(
      final KsqlQueryBuilder queryBuilder) {
    final KeyFormat keyFormat = formats.getKeyFormat();
    final ValueFormat valueFormat = formats.getValueFormat();
    final PhysicalSchema schema = functions.physicalSchemaFactory.apply(
        sourceSchema,
        formats.getOptions()
    );
    final Serde<GenericRow> valueSerde = queryBuilder.buildValueSerde(
        valueFormat.getFormatInfo(),
        schema,
        properties.getQueryContext()
    );
    final Consumed<K, GenericRow> consumed = buildSourceConsumed(
        functions.keySerdeSupplier.buildKeySerde(
            keyFormat, schema, properties.getQueryContext(), queryBuilder),
        valueSerde
    );
    return Pair.of(
        (KeySerde<K>) KsqlConsumed.keySerde(consumed),
        buildKStream(queryBuilder, consumed, functions.metaFieldMapper)
    );
  }

  public static LogicalSchemaWithMetaAndKeyFields getSchemaWithMetaAndKeyFields(
      final String alias,
      final LogicalSchema schema) {
    return LogicalSchemaWithMetaAndKeyFields.fromOriginal(alias, schema);
  }

  interface KeySerdeSupplier<K> {
    KeySerde<K> buildKeySerde(
        KeyFormat format,
        PhysicalSchema schema,
        QueryContext queryContext,
        KsqlQueryBuilder builder
    );
  }

  interface Constructor<K> {
    StreamSource<K> construct(
        ExecutionStepProperties properties,
        String topicName,
        Formats formats,
        TimestampExtractionPolicy timestampPolicy,
        int timestampIndex,
        Optional<AutoOffsetReset> offsetReset,
        LogicalSchema sourceSchema,
        Functions<K> functions
    );
  }

  static class Functions<K> {
    final KeySerdeSupplier<K> keySerdeSupplier;
    final ValueMapperWithKey<K, GenericRow, GenericRow> metaFieldMapper;
    final ValueTransformerSupplier<GenericRow, GenericRow> timestampTransformer;
    final BiFunction<Serde<K>, Serde<GenericRow>, Consumed<K, GenericRow>> consumedFactory;
    final BiFunction<LogicalSchema, Set<SerdeOption>, PhysicalSchema> physicalSchemaFactory;

    Functions(
        final KeySerdeSupplier<K> keySerdeSupplier,
        final ValueMapperWithKey<K, GenericRow, GenericRow> metaFieldMapper,
        final ValueTransformerSupplier<GenericRow, GenericRow> timestampTransformer,
        final BiFunction<Serde<K>, Serde<GenericRow>, Consumed<K, GenericRow>> consumedFactory,
        final BiFunction<LogicalSchema, Set<SerdeOption>, PhysicalSchema> physicalSchemaFactory) {
      this.keySerdeSupplier = Objects.requireNonNull(keySerdeSupplier, "keySerdeSupplier");
      this.metaFieldMapper = Objects.requireNonNull(metaFieldMapper, "mapper");
      this.timestampTransformer
          = Objects.requireNonNull(timestampTransformer, "timestampTransformer");
      this.consumedFactory = Objects.requireNonNull(consumedFactory, "consumedFactory");
      this.physicalSchemaFactory
          = Objects.requireNonNull(physicalSchemaFactory, "physicalSchemaFactory");
    }
  }

  static StreamSource<Windowed<Struct>> createWindowed(
      final QueryContext queryContext,
      final LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
      final String topicName,
      final KeyFormat keyFormat,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options,
      final TimestampExtractionPolicy timestampPolicy,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset,
      final Constructor<Windowed<Struct>> constructor
  ) {
    return constructor.construct(
        new DefaultExecutionStepProperties(
            queryContext.toString(),
            schemaWithMetaAndKeyFields.getSchema(),
            queryContext
        ),
        topicName,
        Formats.of(keyFormat, valueFormat, options),
        timestampPolicy,
        timestampIndex,
        offsetReset,
        schemaWithMetaAndKeyFields.getOriginalSchema(),
        new Functions<>(
            (fmt, schema, ctx, builder) -> builder.buildKeySerde(
                fmt.getFormatInfo(),
                fmt.getWindowInfo().get(),
                schema,
                ctx
            ),
            windowedMapper(schemaWithMetaAndKeyFields.getSchema()),
            new AddTimestampColumn(),
            Consumed::with,
            PhysicalSchema::from
        )
    );
  }

  public static StreamSource<Windowed<Struct>> createWindowed(
      final QueryContext queryContext,
      final LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
      final String topicName,
      final KeyFormat keyFormat,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options,
      final TimestampExtractionPolicy timestampPolicy,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset) {
    return createWindowed(
        queryContext,
        schemaWithMetaAndKeyFields,
        topicName,
        keyFormat,
        valueFormat,
        options,
        timestampPolicy,
        timestampIndex,
        offsetReset,
        StreamSource::new
    );
  }

  static StreamSource<Struct> createNonWindowed(
      final QueryContext queryContext,
      final LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
      final String topicName,
      final KeyFormat keyFormat,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options,
      final TimestampExtractionPolicy timestampPolicy,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset,
      final Constructor<Struct> constructor
  ) {
    return constructor.construct(
        new DefaultExecutionStepProperties(
            queryContext.toString(),
            schemaWithMetaAndKeyFields.getSchema(),
            queryContext
        ),
        topicName,
        Formats.of(keyFormat, valueFormat, options),
        timestampPolicy,
        timestampIndex,
        offsetReset,
        schemaWithMetaAndKeyFields.getOriginalSchema(),
        new Functions<>(
            (fmt, schema, ctx, builder) -> builder.buildKeySerde(
                fmt.getFormatInfo(),
                schema,
                ctx
            ),
            nonWindowedValueMapper(schemaWithMetaAndKeyFields.getSchema()),
            new AddTimestampColumn(),
            Consumed::with,
            PhysicalSchema::from
        )
    );
  }

  public static StreamSource<Struct> createNonWindowed(
      final QueryContext queryContext,
      final LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
      final String topicName,
      final KeyFormat keyFormat,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options,
      final TimestampExtractionPolicy timestampPolicy,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset) {
    return createNonWindowed(
        queryContext,
        schemaWithMetaAndKeyFields,
        topicName,
        keyFormat,
        valueFormat,
        options,
        timestampPolicy,
        timestampIndex,
        offsetReset,
        StreamSource::new
    );
  }

  private static org.apache.kafka.connect.data.Field getKeySchemaSingleField(
      final LogicalSchema schema) {
    if (schema.keySchema().fields().size() != 1) {
      throw new IllegalStateException("Only single key fields are currently supported");
    }
    return schema.keySchema().fields().get(0);
  }

  private static ValueMapperWithKey<Windowed<Struct>, GenericRow, GenericRow> windowedMapper(
      final LogicalSchema schema) {
    final org.apache.kafka.connect.data.Field keyField = getKeySchemaSingleField(schema);
    return (keyStruct, row) -> {
      if (row != null) {
        final Window window = keyStruct.window();
        final long start = window.start();
        final String end = window instanceof SessionWindow ? String.valueOf(window.end()) : "-";
        final Object key = keyStruct.key().get(keyField);
        final String rowKey = String.format("%s : Window{start=%d end=%s}", key, start, end);
        row.getColumns().add(0, rowKey);
      }
      return row;
    };
  }

  private static ValueMapperWithKey<Struct, GenericRow, GenericRow> nonWindowedValueMapper(
      final LogicalSchema schema) {
    final org.apache.kafka.connect.data.Field keyField = getKeySchemaSingleField(schema);
    return (key, row) -> {
      if (row != null) {
        row.getColumns().add(0, key.get(keyField));
      }
      return row;
    };
  }

  private static class AddTimestampColumn
      implements ValueTransformerSupplier<GenericRow, GenericRow> {
    @Override
    public ValueTransformer<GenericRow, GenericRow> get() {
      return new ValueTransformer<GenericRow, GenericRow>() {
        ProcessorContext processorContext;
        @Override
        public void init(final ProcessorContext processorContext) {
          this.processorContext = processorContext;
        }

        @Override
        public GenericRow transform(final GenericRow row) {
          if (row != null) {
            row.getColumns().add(0, processorContext.timestamp());
          }
          return row;
        }

        @Override
        public void close() {
        }
      };
    }
  }
}
