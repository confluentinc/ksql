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
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.timestamp.KsqlTimestampExtractor;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicyFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

public final class SinkBuilder {
  private static final String TIMESTAMP_TRANSFORM_NAME = "ApplyTimestampTransform-";

  private SinkBuilder() {
  }

  public static  <K> void build(
      final LogicalSchema schema,
      final Formats formats,
      final Optional<TimestampColumn> timestampColumn,
      final String topicName,
      final KStream<K, GenericRow> stream,
      final ExecutionKeyFactory<K> executionKeyFactory,
      final QueryContext queryContext,
      final RuntimeBuildContext buildContext
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        schema,
        formats.getKeyFeatures(),
        formats.getValueFeatures()
    );

    final Serde<K> keySerde = executionKeyFactory.buildKeySerde(
        formats.getKeyFormat(),
        physicalSchema,
        queryContext
    );

    final Serde<GenericRow> valueSerde = buildContext.buildValueSerde(
        formats.getValueFormat(),
        physicalSchema,
        queryContext
    );

    final Optional<TimestampProcessorSupplier<K>> tsTransformer = timestampTransformer(
        buildContext,
        queryContext,
        schema,
        timestampColumn
    );

    final KStream<K, GenericRow> transformed = tsTransformer
        .map(t -> stream.process(t, Named.as(TIMESTAMP_TRANSFORM_NAME
            + StreamsUtil.buildOpName(queryContext))))
        .orElse(stream);

    transformed.to(topicName, Produced.with(keySerde, valueSerde));
  }

  private static  <K> Optional<TimestampProcessorSupplier<K>> timestampTransformer(
      final RuntimeBuildContext buildContext,
      final QueryContext queryContext,
      final LogicalSchema sourceSchema,
      final Optional<TimestampColumn> timestampColumn
  ) {
    if (!timestampColumn.isPresent()) {
      return Optional.empty();
    }

    final TimestampExtractionPolicy timestampPolicy = TimestampExtractionPolicyFactory.create(
        buildContext.getKsqlConfig(),
        sourceSchema,
        timestampColumn
    );

    return timestampColumn
        .map(TimestampColumn::getColumn)
        .map(c -> sourceSchema.findColumn(c).orElseThrow(IllegalStateException::new))
        .map(c -> timestampPolicy.create(Optional.of(c)))
        .map(te ->
            new TimestampProcessorSupplier<>(te, buildContext.getProcessingLogger(queryContext)));
  }

  static class TimestampProcessorSupplier<K>
      implements ProcessorSupplier<K, GenericRow, K, GenericRow> {
    private final KsqlTimestampExtractor timestampExtractor;
    private final ProcessingLogger processingLogger;

    TimestampProcessorSupplier(
        final KsqlTimestampExtractor timestampExtractor,
        final ProcessingLogger processingLogger
    ) {
      this.timestampExtractor = requireNonNull(timestampExtractor, "timestampExtractor");
      this.processingLogger = requireNonNull(processingLogger, "processingLogger");
    }

    @Override
    public Processor<K, GenericRow, K, GenericRow> get() {
      return new Processor<K, GenericRow, K, GenericRow>() {
        private ProcessorContext processorContext;

        @Override
        public void init(final ProcessorContext<K, GenericRow> processorContext) {
          this.processorContext = requireNonNull(processorContext, "processorContext");
        }

        @Override
        public void process(final Record<K, GenericRow> record) {
          try {
            final long timestamp;
            if (record.value() == null) {
              timestamp = processorContext.currentStreamTimeMs();
            } else {
              timestamp = timestampExtractor.extract(record.key(), record.value());
            }
            processorContext.forward(record.withTimestamp(timestamp));
          } catch (final Exception e) {
            processingLogger.error(RecordProcessingError
                    .recordProcessingError("Error writing row with extracted timestamp: "
                        + e.getMessage(), e, record.value())
            );
          }
        }


        @Override
        public void close() {
        }
      };
    }
  }
}
