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
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.streams.timestamp.AbstractColumnTimestampExtractor;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicyFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.execution.util.EngineProcessingLogMessageFactory;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
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

    final Optional<TransformTimestamp<K>> tsTransformer = timestampTransformer(
        queryBuilder,
        queryContext,
        schema,
        timestampColumn
    );

    final KStream<K, GenericRow> transformed = tsTransformer
        .map(t -> stream.transform(t, Named.as(StreamsUtil.buildOpName(queryContext))))
        .orElse(stream);

    transformed.to(topicName, Produced.with(keySerde, valueSerde));
  }

  private static  <K> Optional<TransformTimestamp<K>> timestampTransformer(
      final KsqlQueryBuilder queryBuilder,
      final QueryContext queryContext,
      final LogicalSchema sourceSchema,
      final Optional<TimestampColumn> timestampColumn
  ) {
    if (!timestampColumn.isPresent()) {
      return Optional.empty();
    }

    final TimestampExtractionPolicy timestampPolicy = TimestampExtractionPolicyFactory.create(
        queryBuilder.getKsqlConfig(),
        sourceSchema,
        timestampColumn
    );

    return timestampColumn
        .map(TimestampColumn::getColumn)
        .map(c -> sourceSchema.findValueColumn(c).orElseThrow(IllegalStateException::new))
        .map(Column::index)
        .map(timestampPolicy::create)
        .filter(te -> te instanceof AbstractColumnTimestampExtractor)
        .map(te -> new TransformTimestamp<>(
            (AbstractColumnTimestampExtractor)te,
            queryBuilder
                .getProcessingLogContext()
                .getLoggerFactory()
                .getLogger(
                    QueryLoggerUtil.queryLoggerName(
                        queryBuilder.getQueryId(),
                        queryContext
                    )
                )
            )
        );
  }

  static class TransformTimestamp<K>
      implements TransformerSupplier<K, GenericRow, KeyValue<K, GenericRow>> {
    private final AbstractColumnTimestampExtractor timestampExtractor;
    private final ProcessingLogger processingLogger;

    TransformTimestamp(
        final AbstractColumnTimestampExtractor timestampExtractor,
        final ProcessingLogger processingLogger
    ) {
      this.timestampExtractor = requireNonNull(timestampExtractor, "timestampExtractor");
      this.processingLogger = requireNonNull(processingLogger, "processingLogger");
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
          try {
            processorContext.forward(
                key,
                row,
                To.all().withTimestamp(timestampExtractor.extract(row))
            );
          } catch (final Exception e) {
            processingLogger.error(
                EngineProcessingLogMessageFactory
                    .recordProcessingError("Error writing row with extracted timestamp: "
                        + e.getMessage(), e, row)
            );
          }

          return null;
        }


        @Override
        public void close() {
        }
      };
    }
  }
}
