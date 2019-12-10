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

package io.confluent.ksql.execution.streams;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.AbstractStreamSource;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicyFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;

public final class SourceBuilder {

  private SourceBuilder() {
  }

  public static KStreamHolder<Struct> buildStream(
      final KsqlQueryBuilder queryBuilder,
      final StreamSource source,
      final ConsumedFactory consumedFactory
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, source, physicalSchema);

    final Serde<Struct> keySerde = queryBuilder.buildKeySerde(
        source.getFormats().getKeyFormat(),
        physicalSchema,
        source.getProperties().getQueryContext()
    );

    final Consumed<Struct, GenericRow> consumed = buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.LATEST,
        queryBuilder,
        consumedFactory
    );

    final KStream<Struct, GenericRow> kstream = buildKStream(
        source,
        queryBuilder,
        consumed,
        nonWindowedRowKeyGenerator(source.getSourceSchema())
    );

    return new KStreamHolder<>(
        kstream,
        buildSchema(source),
        KeySerdeFactory.unwindowed(queryBuilder)
    );
  }

  static KStreamHolder<Windowed<Struct>> buildWindowedStream(
      final KsqlQueryBuilder queryBuilder,
      final WindowedStreamSource source,
      final ConsumedFactory consumedFactory
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, source, physicalSchema);

    final WindowInfo windowInfo = source.getWindowInfo();
    final Serde<Windowed<Struct>> keySerde = queryBuilder.buildKeySerde(
        source.getFormats().getKeyFormat(),
        windowInfo,
        physicalSchema,
        source.getProperties().getQueryContext()
    );

    final Consumed<Windowed<Struct>, GenericRow> consumed = buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.LATEST,
        queryBuilder,
        consumedFactory
    );

    final KStream<Windowed<Struct>, GenericRow> kstream = buildKStream(
        source,
        queryBuilder,
        consumed,
        windowedRowKeyGenerator(source.getSourceSchema())
    );

    return new KStreamHolder<>(
        kstream,
        buildSchema(source),
        KeySerdeFactory.windowed(queryBuilder, windowInfo)
    );
  }

  public static KTableHolder<Struct> buildTable(
      final KsqlQueryBuilder queryBuilder,
      final TableSource source,
      final ConsumedFactory consumedFactory,
      final MaterializedFactory materializedFactory
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, source, physicalSchema);

    final Serde<Struct> keySerde = queryBuilder.buildKeySerde(
        source.getFormats().getKeyFormat(),
        physicalSchema,
        source.getProperties().getQueryContext()
    );

    final Consumed<Struct, GenericRow> consumed = buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.EARLIEST,
        queryBuilder,
        consumedFactory
    );

    final Materialized<Struct, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        materializedFactory.create(
            keySerde,
            valueSerde,
            tableChangeLogOpName(source.getProperties())
        );

    final KTable<Struct, GenericRow> ktable = buildKTable(
        source,
        queryBuilder,
        consumed,
        nonWindowedRowKeyGenerator(source.getSourceSchema()),
        materialized
    );

    return KTableHolder.unmaterialized(
        ktable,
        buildSchema(source),
        KeySerdeFactory.unwindowed(queryBuilder)
    );
  }

  static KTableHolder<Windowed<Struct>> buildWindowedTable(
      final KsqlQueryBuilder queryBuilder,
      final WindowedTableSource source,
      final ConsumedFactory consumedFactory,
      final MaterializedFactory materializedFactory
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, source, physicalSchema);

    final WindowInfo windowInfo = source.getWindowInfo();
    final Serde<Windowed<Struct>> keySerde = queryBuilder.buildKeySerde(
        source.getFormats().getKeyFormat(),
        windowInfo,
        physicalSchema,
        source.getProperties().getQueryContext()
    );

    final Consumed<Windowed<Struct>, GenericRow> consumed = buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.EARLIEST,
        queryBuilder,
        consumedFactory
    );

    final Materialized<Windowed<Struct>, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        materializedFactory.create(
            keySerde,
            valueSerde,
            tableChangeLogOpName(source.getProperties())
        );

    final KTable<Windowed<Struct>, GenericRow> ktable = buildKTable(
        source,
        queryBuilder,
        consumed,
        windowedRowKeyGenerator(source.getSourceSchema()),
        materialized
    );

    return KTableHolder.unmaterialized(
        ktable,
        buildSchema(source),
        KeySerdeFactory.windowed(queryBuilder, windowInfo)
    );
  }

  private static LogicalSchema buildSchema(final AbstractStreamSource<?> source) {
    return source
        .getSourceSchema()
        .withAlias(source.getAlias())
        .withMetaAndKeyColsInValue();
  }

  private static Serde<GenericRow> getValueSerde(
      final KsqlQueryBuilder queryBuilder,
      final AbstractStreamSource<?> streamSource,
      final PhysicalSchema physicalSchema) {
    return queryBuilder.buildValueSerde(
        streamSource.getFormats().getValueFormat(),
        physicalSchema,
        streamSource.getProperties().getQueryContext()
    );
  }

  private static PhysicalSchema getPhysicalSchema(final AbstractStreamSource<?> streamSource) {
    return PhysicalSchema.from(
        streamSource.getSourceSchema(),
        streamSource.getFormats().getOptions()
    );
  }

  private static <K> KStream<K, GenericRow> buildKStream(
      final AbstractStreamSource<?> streamSource,
      final KsqlQueryBuilder queryBuilder,
      final Consumed<K, GenericRow> consumed,
      final Function<K, Object> rowKeyGenerator
  ) {
    KStream<K, GenericRow> stream = queryBuilder.getStreamsBuilder()
        .stream(streamSource.getTopicName(), consumed);

    return stream
        .transformValues(new AddKeyAndTimestampColumns<>(rowKeyGenerator));
  }

  private static <K> KTable<K, GenericRow> buildKTable(
      final AbstractStreamSource<?> streamSource,
      final KsqlQueryBuilder queryBuilder,
      final Consumed<K, GenericRow> consumed,
      final Function<K, Object> rowKeyGenerator,
      final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized
  ) {
    final KTable<K, GenericRow> table = queryBuilder.getStreamsBuilder()
        .table(streamSource.getTopicName(), consumed, materialized);

    return table
        .transformValues(new AddKeyAndTimestampColumns<>(rowKeyGenerator));
  }

  private static TimestampExtractor timestampExtractor(
      final KsqlConfig ksqlConfig,
      final LogicalSchema sourceSchema,
      final Optional<TimestampColumn> timestampColumn
  ) {
    final TimestampExtractionPolicy timestampPolicy = TimestampExtractionPolicyFactory.create(
        ksqlConfig,
        sourceSchema,
        timestampColumn
    );

    final int timestampIndex = timestampColumn.map(TimestampColumn::getColumn)
        .map(c -> sourceSchema.findValueColumn(c).orElseThrow(IllegalStateException::new))
        .map(Column::index)
        .orElse(-1);

    return timestampPolicy.create(timestampIndex);
  }

  private static <K> Consumed<K, GenericRow> buildSourceConsumed(
      final AbstractStreamSource<?> streamSource,
      final Serde<K> keySerde,
      final Serde<GenericRow> valueSerde,
      final Topology.AutoOffsetReset defaultReset,
      final KsqlQueryBuilder queryBuilder,
      final ConsumedFactory consumedFactory) {
    final TimestampExtractor timestampExtractor = timestampExtractor(
        queryBuilder.getKsqlConfig(),
        streamSource.getSourceSchema(),
        streamSource.getTimestampColumn()
    );
    final Consumed<K, GenericRow> consumed = consumedFactory
        .create(keySerde, valueSerde)
        .withTimestampExtractor(timestampExtractor);
    return consumed.withOffsetResetPolicy(getAutoOffsetReset(defaultReset, queryBuilder));
  }

  private static org.apache.kafka.connect.data.Field getKeySchemaSingleField(
      final LogicalSchema schema) {
    if (schema.keyConnectSchema().fields().size() != 1) {
      throw new IllegalStateException("Only single key fields are currently supported");
    }
    return schema.keyConnectSchema().fields().get(0);
  }

  private static String tableChangeLogOpName(final ExecutionStepPropertiesV1 props) {
    final List<String> parts = props.getQueryContext().getContext();
    Stacker stacker = new Stacker();
    for (final String part : parts.subList(0, parts.size() - 1)) {
      stacker = stacker.push(part);
    }
    return StreamsUtil.buildOpName(stacker.push("Reduce").getQueryContext());
  }

  private static Function<Windowed<Struct>, Object> windowedRowKeyGenerator(
      final LogicalSchema schema
  ) {
    final org.apache.kafka.connect.data.Field keyField = getKeySchemaSingleField(schema);

    return windowedKey -> {
      if (windowedKey == null) {
        return null;
      }

      final Window window = windowedKey.window();
      final long start = window.start();
      final String end = window instanceof SessionWindow ? String.valueOf(window.end()) : "-";
      final Object key = windowedKey.key().get(keyField);
      return String.format("%s : Window{start=%d end=%s}", key, start, end);
    };
  }

  private static Function<Struct, Object> nonWindowedRowKeyGenerator(
      final LogicalSchema schema
  ) {
    final org.apache.kafka.connect.data.Field keyField = getKeySchemaSingleField(schema);
    return key -> {
      if (key == null) {
        return null;
      }

      return key.get(keyField);
    };
  }

  private static class AddKeyAndTimestampColumns<K>
      implements ValueTransformerWithKeySupplier<K, GenericRow, GenericRow> {

    private final Function<K, Object> rowKeyGenerator;

    AddKeyAndTimestampColumns(final Function<K, Object> rowKeyGenerator) {
      this.rowKeyGenerator = requireNonNull(rowKeyGenerator, "rowKeyGenerator");
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

          row.getColumns().add(0, processorContext.timestamp());
          row.getColumns().add(1, rowKeyGenerator.apply(key));
          return row;
        }

        @Override
        public void close() {
        }
      };
    }
  }

  private static Topology.AutoOffsetReset getAutoOffsetReset(
      final Topology.AutoOffsetReset defaultValue,
      final KsqlQueryBuilder queryBuilder) {
    final Object offestReset = queryBuilder.getKsqlConfig()
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
}
