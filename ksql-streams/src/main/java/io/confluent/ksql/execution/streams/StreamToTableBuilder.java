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
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.StreamToTable;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeySerde;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

public final class StreamToTableBuilder {
  private StreamToTableBuilder() {
  }

  public static <K> KTableHolder<K> build(
      final KStreamHolder<K> sourceStream,
      final StreamToTable<K> streamToTable,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory) {
    final QueryContext queryContext = streamToTable.getProperties().getQueryContext();
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        sourceStream.getSchema(),
        streamToTable.getFormats().getOptions()
    );
    final Serde<GenericRow> valueSerde = queryBuilder.buildValueSerde(
        streamToTable.getFormats().getValueFormat(),
        physicalSchema,
        queryContext
    );
    final KeySerde<K> keySerde = sourceStream.getKeySerdeFactory().buildKeySerde(
        streamToTable.getFormats().getKeyFormat(),
        physicalSchema,
        queryContext
    );
    final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        materializedFactory.create(
            keySerde,
            valueSerde,
            StreamsUtil.buildOpName(queryContext)
        );
    final KTable<K, GenericRow> table = sourceStream.getStream()
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
    return KTableHolder.unmaterialized(
        table,
        sourceStream.getSchema(),
        sourceStream.getKeySerdeFactory()
    );
  }
}
