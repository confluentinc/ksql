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
import io.confluent.ksql.execution.plan.AbstractStreamSource;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.execution.timestamp.TimestampExtractionPolicyFactory;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;

public final class StreamSourceBuilder {
  private StreamSourceBuilder() {
  }

  public static KStreamHolder<Struct> build(
      final KsqlQueryBuilder queryBuilder,
      final StreamSource streamSource,
      final ConsumedFactory consumedFactory
  ) {
    final KeyFormat keyFormat = streamSource.getFormats().getKeyFormat();
    if (keyFormat.getWindowInfo().isPresent()) {
      throw new IllegalArgumentException("Windowed source");
    }

    final PhysicalSchema physicalSchema = getPhysicalSchema(streamSource);

    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, streamSource, physicalSchema);

    final KeySerde<Struct> keySerde = queryBuilder.buildKeySerde(
        keyFormat.getFormatInfo(),
        physicalSchema,
        streamSource.getProperties().getQueryContext()
    );

    final Consumed<Struct, GenericRow> consumed = buildSourceConsumed(
        streamSource,
        keySerde,
        valueSerde,
        queryBuilder,
        consumedFactory
    );

    final KStream<Struct, GenericRow> kstream = buildKStream(
        streamSource,
        queryBuilder,
        consumed,
        nonWindowedRowKeyGenerator(streamSource.getSourceSchema())
    );

    return new KStreamHolder<>(
        kstream,
        streamSource
            .getSourceSchema()
            .withAlias(streamSource.getAlias())
            .withMetaAndKeyColsInValue(),
        (fmt, schema, ctx) -> queryBuilder.buildKeySerde(fmt.getFormatInfo(), schema, ctx)
    );
  }

  static KStreamHolder<Windowed<Struct>> buildWindowed(
      final KsqlQueryBuilder queryBuilder,
      final WindowedStreamSource streamSource,
      final ConsumedFactory consumedFactory
  ) {
    final KeyFormat keyFormat = streamSource.getFormats().getKeyFormat();
    if (!keyFormat.getWindowInfo().isPresent()) {
      throw new IllegalArgumentException("Not windowed source");
    }

    final PhysicalSchema physicalSchema = getPhysicalSchema(streamSource);

    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, streamSource, physicalSchema);

    final KeySerde<Windowed<Struct>> keySerde = queryBuilder.buildKeySerde(
        keyFormat.getFormatInfo(),
        keyFormat.getWindowInfo().get(),
        physicalSchema,
        streamSource.getProperties().getQueryContext()
    );

    final Consumed<Windowed<Struct>, GenericRow> consumed = buildSourceConsumed(
        streamSource,
        keySerde,
        valueSerde,
        queryBuilder,
        consumedFactory
    );

    final KStream<Windowed<Struct>, GenericRow> kstream = buildKStream(
        streamSource,
        queryBuilder,
        consumed,
        windowedRowKeyGenerator(streamSource.getSourceSchema())
    );

    return new KStreamHolder<>(
        kstream,
        streamSource.getSourceSchema()
            .withAlias(streamSource.getAlias())
            .withMetaAndKeyColsInValue(),
        (fmt, schema, ctx) -> queryBuilder.buildKeySerde(
            fmt.getFormatInfo(),
            fmt.getWindowInfo().get(),
            schema,
            ctx
        )
    );
  }

  private static Serde<GenericRow> getValueSerde(
      final KsqlQueryBuilder queryBuilder,
      final AbstractStreamSource<?> streamSource,
      final PhysicalSchema physicalSchema) {
    return queryBuilder.buildValueSerde(
        streamSource.getFormats().getValueFormat().getFormatInfo(),
        physicalSchema,
        streamSource.getProperties().getQueryContext()
    );
  }

  private static PhysicalSchema getPhysicalSchema(final AbstractStreamSource streamSource) {
    return PhysicalSchema.from(
        streamSource.getSourceSchema(),
        streamSource.getFormats().getOptions()
    );
  }

  private static <K> KStream<K, GenericRow> buildKStream(
      final AbstractStreamSource streamSource,
      final KsqlQueryBuilder queryBuilder,
      final Consumed<K, GenericRow> consumed,
      final Function<K, String> rowKeyGenerator
  ) {
    KStream<K, GenericRow> stream = queryBuilder.getStreamsBuilder()
        .stream(streamSource.getTopicName(), consumed);

    return stream
        .transformValues(new AddKeyAndTimestampColumns<>(rowKeyGenerator));
  }

  private static TimestampExtractor timestampExtractor(
      final KsqlConfig ksqlConfig,
      final LogicalSchema sourceSchema,
      final Optional<TimestampColumn> timestampColumn) {
    final TimestampExtractionPolicy timestampPolicy = TimestampExtractionPolicyFactory.create(
        ksqlConfig,
        sourceSchema,
        timestampColumn
    );
    final int timestampIndex = timestampColumn.map(TimestampColumn::getColumn)
        .map(c -> sourceSchema.valueColumnIndex(c).orElseThrow(IllegalStateException::new))
        .orElse(-1);
    return timestampPolicy.create(timestampIndex);
  }

  private static <K> Consumed<K, GenericRow> buildSourceConsumed(
      final AbstractStreamSource<?> streamSource,
      final Serde<K> keySerde,
      final Serde<GenericRow> valueSerde,
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
    return streamSource.getOffsetReset()
        .map(consumed::withOffsetResetPolicy)
        .orElse(consumed);
  }

  private static org.apache.kafka.connect.data.Field getKeySchemaSingleField(
      final LogicalSchema schema) {
    if (schema.keyConnectSchema().fields().size() != 1) {
      throw new IllegalStateException("Only single key fields are currently supported");
    }
    return schema.keyConnectSchema().fields().get(0);
  }

  private static Function<Windowed<Struct>, String> windowedRowKeyGenerator(
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

  private static Function<Struct, String> nonWindowedRowKeyGenerator(
      final LogicalSchema schema
  ) {
    final org.apache.kafka.connect.data.Field keyField = getKeySchemaSingleField(schema);
    return key -> {
      if (key == null) {
        return null;
      }

      final Object k = key.get(keyField);
      return k == null
          ? null
          : k.toString();
    };
  }

  private static class AddKeyAndTimestampColumns<K>
      implements ValueTransformerWithKeySupplier<K, GenericRow, GenericRow> {

    private final Function<K, String> rowKeyGenerator;

    AddKeyAndTimestampColumns(final Function<K, String> rowKeyGenerator) {
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
}
