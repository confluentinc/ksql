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

import static io.confluent.ksql.execution.streams.SourceBuilderUtils.buildSchema;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.buildSourceConsumed;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getKeySerde;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getPhysicalSchema;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getValueSerde;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.tableChangeLogOpName;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.Collection;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

abstract class SourceBuilderBase {

  KTableHolder<GenericKey> buildTable(
      final RuntimeBuildContext buildContext,
      final SourceStep<KTableHolder<GenericKey>> source,
      final ConsumedFactory consumedFactory,
      final MaterializedFactory materializedFactory,
      final PlanInfo planInfo
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(buildContext, source, physicalSchema);

    final Serde<GenericKey> keySerde = getKeySerde(
        source,
        physicalSchema,
        buildContext
    );

    final Consumed<GenericKey, GenericRow> consumed = buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.EARLIEST,
        buildContext,
        consumedFactory
    );

    final String stateStoreName = tableChangeLogOpName(source.getProperties());

    final Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> materialized
        = buildTableMaterialized(
        source,
        buildContext,
        materializedFactory,
        keySerde,
        valueSerde,
        stateStoreName
    );

    final KTable<GenericKey, GenericRow> ktable = buildKTable(
        source,
        buildContext,
        consumed,
        GenericKey::values,
        materialized,
        valueSerde,
        stateStoreName,
        planInfo
    );

    final LogicalSchema stateStoreSchema = source.getSourceSchema()
        .withPseudoColumnsToMaterialize(source.getPseudoColumnVersion());

    return KTableHolder.materialized(
        ktable,
        buildSchema(source, false),
        ExecutionKeyFactory.unwindowed(buildContext),
        MaterializationInfo.builder(stateStoreName, stateStoreSchema)
    );
  }

  abstract Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>>
      buildTableMaterialized(
      SourceStep<KTableHolder<GenericKey>> source,
      RuntimeBuildContext buildContext,
      MaterializedFactory materializedFactory,
      Serde<GenericKey> keySerde,
      Serde<GenericRow> valueSerde,
      String stateStoreName
  );

  abstract <K> KTable<K, GenericRow> buildKTable(
      SourceStep<?> streamSource,
      RuntimeBuildContext buildContext,
      Consumed<K, GenericRow> consumed,
      Function<K, Collection<?>> keyGenerator,
      Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized,
      Serde<GenericRow> valueSerde,
      String stateStoreName,
      PlanInfo planInfo
  );

}
