/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.StreamToTable;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.ValueFormat;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

public final class StreamToTableBuilder {
  private StreamToTableBuilder() {
  }

  public static KTable<Object, GenericRow> build(
      final KStream<Object, GenericRow> sourceStream,
      final StreamToTable<KStream<Object, GenericRow>, KTable<Object, GenericRow>> streamToTable,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory) {
    final QueryContext queryContext = streamToTable.getProperties().getQueryContext();
    final ExecutionStep<KStream<Object, GenericRow>> sourceStep = streamToTable.getSource();
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        sourceStep.getProperties().getSchema(),
        streamToTable.getFormats().getOptions()
    );
    final ValueFormat valueFormat = streamToTable.getFormats().getValueFormat();
    final Serde<GenericRow> valueSerde = queryBuilder.buildValueSerde(
        valueFormat.getFormatInfo(),
        physicalSchema,
        queryContext
    );
    final KeyFormat keyFormat = streamToTable.getFormats().getKeyFormat();
    final KeySerde<Object> keySerde = buildKeySerde(
        keyFormat,
        queryBuilder,
        physicalSchema,
        queryContext
    );
    final Materialized<Object, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        materializedFactory.create(
            keySerde,
            valueSerde,
            StreamsUtil.buildOpName(queryContext)
        );
    return  sourceStream
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
  }

  @SuppressWarnings("unchecked")
  private static KeySerde<Object> buildKeySerde(
      final KeyFormat keyFormat,
      final KsqlQueryBuilder queryBuilder,
      final PhysicalSchema physicalSchema,
      final QueryContext queryContext
  ) {
    if (keyFormat.isWindowed()) {
      return (KeySerde) queryBuilder.buildKeySerde(
          keyFormat.getFormatInfo(),
          keyFormat.getWindowInfo().get(),
          physicalSchema,
          queryContext
      );
    } else {
      return (KeySerde) queryBuilder.buildKeySerde(
          keyFormat.getFormatInfo(),
          physicalSchema,
          queryContext
      );
    }
  }
}
