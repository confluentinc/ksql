/*
 * Copyright 2020 Confluent Inc.
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
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;

import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

public final class SinkBuilder {
  private SinkBuilder() {
  }

  public static  <K> void build(
      final LogicalSchema schema,
      final Formats formats,
      final Optional<TimestampColumn> timestampColumn,
      final String topicName,
      final KStream<K, GenericRow> stream,
      final KeySerdeFactory<K> keySerdeFactory,
      final QueryContext queryContext,
      final KsqlQueryBuilder queryBuilder
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(schema, formats.getOptions());

    final Serde<K> keySerde = keySerdeFactory.buildKeySerde(
        formats.getKeyFormat(),
        physicalSchema,
        queryContext
    );

    final Serde<GenericRow> valueSerde = queryBuilder.buildValueSerde(
        formats.getValueFormat(),
        physicalSchema,
        queryContext
    );

    final int timestampColumnIndex = timestampColumn.map(TimestampColumn::getColumn)
        .map(c -> schema.findValueColumn(c).orElseThrow(IllegalStateException::new))
        .map(Column::index)
        .orElse(-1);

    stream.transform(new TransformTimestamp<>(timestampColumnIndex))
        .to(topicName, Produced.with(keySerde, valueSerde));
  }

  static class TransformTimestamp<K>
      implements TransformerSupplier<K, GenericRow, KeyValue<K, GenericRow>> {
    private final int timestampColumnIndex;

    TransformTimestamp(final int timestampColumnIndex) {
      this.timestampColumnIndex = timestampColumnIndex;
    }

    @Override
    public boolean equals(final Object o) {
      if (o == null || !(o instanceof TransformTimestamp)) {
        return false;
      }

      final TransformTimestamp that = (TransformTimestamp)o;
      return timestampColumnIndex == that.timestampColumnIndex;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(timestampColumnIndex);
    }

    @Override
    public Transformer<K, GenericRow, KeyValue<K, GenericRow>> get() {
      return new Transformer<K, GenericRow, KeyValue<K, GenericRow>>() {
        private ProcessorContext processorContext;

        @Override
        public void init(final ProcessorContext processorContext) {
          this.processorContext = requireNonNull(processorContext, "processorContext");
        }

        @Override
        public KeyValue<K, GenericRow> transform(final K key, final GenericRow row) {
          if (timestampColumnIndex >= 0 && row.get(timestampColumnIndex) instanceof Long) {
            processorContext.forward(
                key,
                row,
                To.all().withTimestamp((long) row.get(timestampColumnIndex))
            );

            return null;
          }

          return KeyValue.pair(key, row);
        }


        @Override
        public void close() {
        }
      };
    }
  }
}
