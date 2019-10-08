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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.plan.AbstractStreamSource;
import io.confluent.ksql.execution.plan.ExecutionStepProperties;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
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
      final StreamSource streamSource
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(streamSource);
    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, streamSource, physicalSchema);
    final KeyFormat keyFormat = streamSource.getFormats().getKeyFormat();
    final ExecutionStepProperties properties = streamSource.getProperties();
    final Consumed<Struct, GenericRow> consumed = buildSourceConsumed(
        streamSource,
        queryBuilder.buildKeySerde(
            keyFormat.getFormatInfo(),
            physicalSchema,
            properties.getQueryContext()
        ),
        valueSerde
    );
    final KStream<Struct, GenericRow> kstream = buildKStream(
        streamSource,
        queryBuilder,
        consumed,
        nonWindowedValueMapper(streamSource.getSourceSchema())
    );
    return new KStreamHolder<>(
        kstream,
        (fmt, schema, ctx) -> queryBuilder.buildKeySerde(fmt.getFormatInfo(), schema, ctx)
    );
  }

  public static KStreamHolder<Windowed<Struct>> buildWindowed(
      final KsqlQueryBuilder queryBuilder,
      final WindowedStreamSource streamSource
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(streamSource);
    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, streamSource, physicalSchema);
    final KeyFormat keyFormat = streamSource.getFormats().getKeyFormat();
    final ExecutionStepProperties properties = streamSource.getProperties();
    final Consumed<Windowed<Struct>, GenericRow> consumed = buildSourceConsumed(
        streamSource,
        queryBuilder.buildKeySerde(
            keyFormat.getFormatInfo(),
            keyFormat.getWindowInfo().get(),
            physicalSchema,
            properties.getQueryContext()
        ),
        valueSerde
    );
    final KStream<Windowed<Struct>, GenericRow> kstream = buildKStream(
        streamSource,
        queryBuilder,
        consumed,
        windowedMapper(streamSource.getSourceSchema())
    );
    return new KStreamHolder<>(
        kstream,
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
      final ValueMapperWithKey<K, GenericRow, GenericRow> mapper) {
    return queryBuilder.getStreamsBuilder()
        // 1. Create a KStream on the changelog topic.
        .stream(streamSource.getTopicName(), consumed)
        // 2. mapValues to add the ROWKEY column
        .mapValues(mapper)
        // 3. transformValues to add the ROWTIME column. transformValues is required to access the
        //    streams ProcessorContext which has the timestamp for the record.
        .transformValues(new AddTimestampColumn());
  }

  private static <K> Consumed<K, GenericRow> buildSourceConsumed(
      final AbstractStreamSource<?> streamSource,
      final Serde<K> keySerde,
      final Serde<GenericRow> valueSerde) {
    final TimestampExtractionPolicy timestampPolicy = streamSource.getTimestampPolicy();
    final int timestampIndex = streamSource.getTimestampIndex();
    final TimestampExtractor timestampExtractor = timestampPolicy.create(timestampIndex);
    final Consumed<K, GenericRow> consumed = Consumed
        .with(keySerde, valueSerde)
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

  public static KeySerde<Windowed<Struct>> getWindowedKeySerde(
      final KsqlQueryBuilder ksqlQueryBuilder,
      final AbstractStreamSource<?> streamSource) {
    return ksqlQueryBuilder.buildKeySerde(
        streamSource.getFormats().getKeyFormat().getFormatInfo(),
        streamSource.getFormats().getKeyFormat().getWindowInfo().get(),
        getPhysicalSchema(streamSource),
        streamSource.getProperties().getQueryContext()
    );
  }

  public static KeySerde<Struct> getKeySerde(
      final KsqlQueryBuilder ksqlQueryBuilder,
      final AbstractStreamSource<?> streamSource) {
    return ksqlQueryBuilder.buildKeySerde(
        streamSource.getFormats().getKeyFormat().getFormatInfo(),
        getPhysicalSchema(streamSource),
        streamSource.getProperties().getQueryContext()
    );
  }
}
