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
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeySerde;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

final class AggregateBuilderUtils {
  private AggregateBuilderUtils() {
  }

  static Materialized<Struct, GenericRow, KeyValueStore<Bytes, byte[]>> buildMaterialized(
      final QueryContext queryContext,
      final LogicalSchema aggregateSchema,
      final Formats formats,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory) {
    final PhysicalSchema physicalAggregationSchema = PhysicalSchema.from(
        aggregateSchema,
        formats.getOptions()
    );
    final KeySerde<Struct> keySerde = queryBuilder.buildKeySerde(
        formats.getKeyFormat(),
        physicalAggregationSchema,
        queryContext
    );
    final Serde<GenericRow> valueSerde = queryBuilder.buildValueSerde(
        formats.getValueFormat(),
        physicalAggregationSchema,
        queryContext
    );
    return materializedFactory.create(keySerde, valueSerde, StreamsUtil.buildOpName(queryContext));
  }

  static MaterializationInfo.Builder materializationInfoBuilder(
      final KudafAggregator aggregator,
      final QueryContext queryContext,
      final LogicalSchema aggregationSchema,
      final LogicalSchema outputSchema
  ) {
    final Function<GenericRow, GenericRow> resultMapper = aggregator
        .getResultMapper()::apply;

    return MaterializationInfo.builder(StreamsUtil.buildOpName(queryContext), aggregationSchema)
        .map(pl -> (k, v) -> resultMapper.apply(v), outputSchema, "AGG-PROJECT");
  }
}
