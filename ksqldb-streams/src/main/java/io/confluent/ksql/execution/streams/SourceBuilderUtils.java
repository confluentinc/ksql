/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicyFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;

final class SourceBuilderUtils {

  private static final String MATERIALIZE_OP_NAME = "Materialized";

  private static final Collection<?> NULL_WINDOWED_KEY_COLUMNS = Collections.unmodifiableList(
      Arrays.asList(null, null, null)
  );

  private SourceBuilderUtils() {
  }

  static LogicalSchema buildSchema(
      final SourceStep<?> source,
      final boolean windowed
  ) {
    return source
        .getSourceSchema()
        .withPseudoAndKeyColsInValue(windowed, source.getPseudoColumnVersion());
  }

  static Serde<GenericRow> getValueSerde(
      final RuntimeBuildContext buildContext,
      final FormatInfo formatInfo,
      final PhysicalSchema physicalSchema,
      final QueryContext queryContext
  ) {
    return buildContext.buildValueSerde(
        formatInfo,
        physicalSchema,
        queryContext
    );
  }

  static Serde<GenericRow> getValueSerde(
      final RuntimeBuildContext buildContext,
      final SourceStep<?> streamSource,
      final PhysicalSchema physicalSchema
  ) {
    return getValueSerde(
        buildContext,
        streamSource.getFormats().getValueFormat(),
        physicalSchema,
        streamSource.getProperties().getQueryContext()
    );
  }

  static Serde<GenericKey> getKeySerde(
      final FormatInfo formatInfo,
      final PhysicalSchema physicalSchema,
      final RuntimeBuildContext buildContext,
      final QueryContext queryContext
  ) {
    return buildContext.buildKeySerde(
        formatInfo,
        physicalSchema,
        queryContext
    );
  }

  static Serde<GenericKey> getKeySerde(
      final SourceStep<?> step,
      final PhysicalSchema physicalSchema,
      final RuntimeBuildContext buildContext
  ) {
    return getKeySerde(
        step.getFormats().getKeyFormat(),
        physicalSchema,
        buildContext,
        step.getProperties().getQueryContext()
    );
  }

  static Serde<Windowed<GenericKey>> getWindowedKeySerde(
      final SourceStep<?> step,
      final PhysicalSchema physicalSchema,
      final RuntimeBuildContext buildContext,
      final WindowInfo windowInfo,
      final QueryContext queryContext
  ) {
    return buildContext.buildKeySerde(
        step.getFormats().getKeyFormat(),
        windowInfo,
        physicalSchema,
        queryContext
    );
  }

  static Serde<Windowed<GenericKey>> getWindowedKeySerde(
      final SourceStep<?> step,
      final PhysicalSchema physicalSchema,
      final RuntimeBuildContext buildContext,
      final WindowInfo windowInfo
  ) {
    return getWindowedKeySerde(
        step,
        physicalSchema,
        buildContext,
        windowInfo,
        step.getProperties().getQueryContext()
    );
  }

  static PhysicalSchema getPhysicalSchema(final SourceStep<?> streamSource) {
    return PhysicalSchema.from(
        streamSource.getSourceSchema(),
        streamSource.getFormats().getKeyFeatures(),
        streamSource.getFormats().getValueFeatures()
    );
  }

  static StaticTopicSerde.Callback getRegisterCallback(
      final RuntimeBuildContext buildContext,
      final FormatInfo valueFormat
  ) {
    final boolean schemaRegistryEnabled = !buildContext
        .getKsqlConfig()
        .getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
        .isEmpty();

    final boolean useSR = FormatFactory
        .fromName(valueFormat.getFormat())
        .supportsFeature(SerdeFeature.SCHEMA_INFERENCE);

    if (!schemaRegistryEnabled || !useSR) {
      return (t1, t2, data) -> { };
    }

    return new RegisterSchemaCallback(buildContext.getServiceContext().getSchemaRegistryClient());
  }

  /**
   * This code mirrors the logic that generates the name for changelog topics
   * in kafka streams, which follows the pattern:
   * <pre>
   *    applicationID + "-" + stateStoreName + "-changelog".
   * </pre>
   */
  static String changelogTopic(
      final RuntimeBuildContext buildContext,
      final String stateStoreName
  ) {
    return buildContext.getApplicationId()
        + "-"
        + stateStoreName
        + "-changelog";
  }

  static TimestampExtractor timestampExtractor(
      final KsqlConfig ksqlConfig,
      final LogicalSchema sourceSchema,
      final Optional<TimestampColumn> timestampColumn,
      final SourceStep<?> streamSource,
      final RuntimeBuildContext buildContext
  ) {
    final TimestampExtractionPolicy timestampPolicy = TimestampExtractionPolicyFactory.create(
        ksqlConfig,
        sourceSchema,
        timestampColumn
    );

    final Optional<Column> tsColumn = timestampColumn.map(TimestampColumn::getColumn)
        .map(c -> sourceSchema.findColumn(c).orElseThrow(IllegalStateException::new));

    final QueryContext queryContext = streamSource.getProperties().getQueryContext();

    return timestampPolicy.create(
        tsColumn,
        ksqlConfig.getBoolean(KsqlConfig.KSQL_TIMESTAMP_THROW_ON_INVALID),
        buildContext.getProcessingLogger(queryContext)
    );
  }

  static <K> Consumed<K, GenericRow> buildSourceConsumed(
      final SourceStep<?> streamSource,
      final Serde<K> keySerde,
      final Serde<GenericRow> valueSerde,
      final Topology.AutoOffsetReset defaultReset,
      final RuntimeBuildContext buildContext,
      final ConsumedFactory consumedFactory) {
    final TimestampExtractor timestampExtractor = timestampExtractor(
        buildContext.getKsqlConfig(),
        streamSource.getSourceSchema(),
        streamSource.getTimestampColumn(),
        streamSource,
        buildContext
    );
    final Consumed<K, GenericRow> consumed = consumedFactory
        .create(keySerde, valueSerde)
        .withTimestampExtractor(timestampExtractor);
    return consumed.withOffsetResetPolicy(getAutoOffsetReset(defaultReset, buildContext));
  }

  static String tableChangeLogOpName(final ExecutionStepPropertiesV1 props) {
    final List<String> parts = props.getQueryContext().getContext();
    Stacker stacker = new Stacker();
    for (final String part : parts.subList(0, parts.size() - 1)) {
      stacker = stacker.push(part);
    }
    return StreamsUtil.buildOpName(stacker.push("Reduce").getQueryContext());
  }

  static Function<Windowed<GenericKey>, Collection<?>> windowedKeyGenerator(
      final LogicalSchema schema
  ) {
    if (schema.key().isEmpty()) {
      throw new IllegalStateException("Windowed sources require a key column");
    }

    return windowedKey -> {
      if (windowedKey == null) {
        return NULL_WINDOWED_KEY_COLUMNS;
      }

      final Window window = windowedKey.window();
      final GenericKey key = windowedKey.key();

      final List<Object> keys = new ArrayList<>(schema.key().size() + 2);
      keys.addAll(key.values());
      keys.add(window.start());
      keys.add(window.end());
      return Collections.unmodifiableCollection(keys);
    };
  }

  static Topology.AutoOffsetReset getAutoOffsetReset(
      final Topology.AutoOffsetReset defaultValue,
      final RuntimeBuildContext buildContext) {
    final Object offestReset = buildContext.getKsqlConfig()
        .getKsqlStreamConfigProps()
        .get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    if (offestReset == null) {
      return defaultValue;
    }

    try {
      return AutoOffsetReset.valueOf(offestReset.toString().toUpperCase());
    } catch (final Exception e) {
      throw new ConfigException(
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          offestReset,
          "Unknown value"
      );
    }
  }

  static QueryContext addMaterializedContext(final SourceStep<?> step) {
    return QueryContext.Stacker.of(
        step.getProperties().getQueryContext())
        .push(MATERIALIZE_OP_NAME).getQueryContext();
  }

  static List<Struct> createHeaderData(final Headers headers) {
    return Arrays.stream(headers.toArray())
        .map(header -> new Struct(SchemaBuilder.struct()
            .field("KEY", Schema.OPTIONAL_STRING_SCHEMA)
            .field("VALUE", Schema.OPTIONAL_BYTES_SCHEMA)
            .optional()
            .build())
            .put("KEY", header.key())
            .put("VALUE", header.value() == null ? null : ByteBuffer.wrap(header.value())))
        .collect(Collectors.toList());
  }

  static ByteBuffer extractHeader(final Headers headers, final String key) {
    final Header header = headers.lastHeader(key);
    return header == null || header.value() == null
        ? null
        : ByteBuffer.wrap(header.value());
  }

  static class AddKeyAndPseudoColumns<K>
      implements ValueTransformerWithKeySupplier<K, GenericRow, GenericRow> {

    private final Function<K, Collection<?>> keyGenerator;
    private final int pseudoColumnVersion;
    private final List<Column> headerColumns;

    AddKeyAndPseudoColumns(
        final Function<K, Collection<?>> keyGenerator,
        final int pseudoColumnVersion,
        final List<Column> headerColumns
    ) {
      this.keyGenerator = requireNonNull(keyGenerator, "keyGenerator");
      this.pseudoColumnVersion = pseudoColumnVersion;
      this.headerColumns = headerColumns;
    }

    @Override
    public ValueTransformerWithKey<K, GenericRow, GenericRow> get() {
      return new ValueTransformerWithKey<K, GenericRow, GenericRow>() {
        private ProcessorContext processorContext;

        @Override
        public void init(final ProcessorContext processorContext) {
          this.processorContext = requireNonNull(processorContext, "processorContext");
        }

        @Override
        public GenericRow transform(final K key, final GenericRow row) {
          if (row == null) {
            return row;
          }

          final Collection<?> keyColumns = keyGenerator.apply(key);

          final int numPseudoColumns = SystemColumns
              .pseudoColumnNames(pseudoColumnVersion).size();

          row.ensureAdditionalCapacity(numPseudoColumns + keyColumns.size() + headerColumns.size());

          for (final Column col : headerColumns) {
            if (col.headerKey().isPresent()) {
              row.append(extractHeader(processorContext.headers(), col.headerKey().get()));
            } else {
              row.append(createHeaderData(processorContext.headers()));
            }
          }

          if (pseudoColumnVersion >= SystemColumns.ROWTIME_PSEUDOCOLUMN_VERSION) {
            final long timestamp = processorContext.timestamp();
            row.append(timestamp);
          }

          if (pseudoColumnVersion >= SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION) {
            final int partition = processorContext.partition();
            final long offset = processorContext.offset();
            row.append(partition);
            row.append(offset);
          }

          row.appendAll(keyColumns);
          return row;
        }

        @Override
        public void close() {
        }
      };
    }

    /**
     * Note: It is duplicate code of AddKeyAndPseudoColumn's transformer
     * @param <K> the key type
     */
    static class AddKeyAndPseudoColumnsProcessor<K>
        implements FixedKeyProcessor<K, GenericRow, GenericRow> {

      private final Function<K, Collection<?>> keyGenerator;
      private final int pseudoColumnVersion;
      private final List<Column> headerColumns;
      private FixedKeyProcessorContext<K, GenericRow> processorContext;

      AddKeyAndPseudoColumnsProcessor(
          final Function<K, Collection<?>> keyGenerator,
          final int pseudoColumnVersion,
          final List<Column> headerColumns
      ) {
        this.keyGenerator = requireNonNull(keyGenerator, "keyGenerator");
        this.pseudoColumnVersion = pseudoColumnVersion;
        this.headerColumns = headerColumns;
      }

      @Override
      public void init(final FixedKeyProcessorContext<K, GenericRow> processorContext) {
        this.processorContext = requireNonNull(processorContext, "processorContext");
      }

      @Override
      public void process(final FixedKeyRecord<K, GenericRow> record) {
        final K key = record.key();
        final GenericRow row = record.value();

        if (row == null) {
          processorContext.forward(record);
          return;
        }

        final Collection<?> keyColumns = keyGenerator.apply(key);

        final int numPseudoColumns = SystemColumns
            .pseudoColumnNames(pseudoColumnVersion).size();

        row.ensureAdditionalCapacity(numPseudoColumns + keyColumns.size() + headerColumns.size());

        for (final Column col : headerColumns) {
          if (col.headerKey().isPresent()) {
            row.append(extractHeader(record.headers(), col.headerKey().get()));
          } else {
            row.append(createHeaderData(record.headers()));
          }
        }

        if (pseudoColumnVersion >= SystemColumns.ROWTIME_PSEUDOCOLUMN_VERSION) {
          final long timestamp = record.timestamp();
          row.append(timestamp);
        }

        if (pseudoColumnVersion >= SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION) {
          final RecordMetadata recordMetadata = processorContext.recordMetadata().get();
          final int partition = recordMetadata.partition();
          final long offset = recordMetadata.offset();
          row.append(partition);
          row.append(offset);
        }

        row.appendAll(keyColumns);
        processorContext.forward(record.withValue(row));
      }
    }
  }
}
