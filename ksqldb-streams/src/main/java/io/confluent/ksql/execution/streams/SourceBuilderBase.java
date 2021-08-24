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

import static io.confluent.ksql.execution.streams.SourceBuilderUtils.buildKeySerde;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.buildSchema;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.buildSourceConsumed;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getPhysicalSchema;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getValueSerde;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.tableChangeLogOpName;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.windowedKeyGenerator;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.plan.WindowedTableSourceV1;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Collection;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Windowed;
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

    final Serde<GenericKey> keySerde = buildKeySerde(
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
        valueSerde
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

    return KTableHolder.materialized(
        ktable,
        buildSchema(source, false),
        ExecutionKeyFactory.unwindowed(buildContext),
        MaterializationInfo.builder(stateStoreName, physicalSchema.logicalSchema())
    );
  }

  KTableHolder<Windowed<GenericKey>> buildWindowedTable(
      final RuntimeBuildContext buildContext,
      final SourceStep<KTableHolder<Windowed<GenericKey>>> source,
      final ConsumedFactory consumedFactory,
      final MaterializedFactory materializedFactory,
      final PlanInfo planInfo
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(buildContext, source, physicalSchema);

    final WindowInfo windowInfo;

    if (source instanceof WindowedTableSourceV1) {
      windowInfo = ((WindowedTableSourceV1) source).getWindowInfo();
    } else if (source instanceof WindowedTableSource) {
      windowInfo = ((WindowedTableSource) source).getWindowInfo();
    } else {
      throw new IllegalArgumentException("Expected a version of WindowedTableSource");
    }

    final Serde<Windowed<GenericKey>> keySerde = SourceBuilderUtils.buildWindowedKeySerde(
        source,
        physicalSchema,
        buildContext,
        windowInfo
    );

    final Consumed<Windowed<GenericKey>, GenericRow> consumed = buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.EARLIEST,
        buildContext,
        consumedFactory
    );

    final String stateStoreName = tableChangeLogOpName(source.getProperties());

    final Materialized<Windowed<GenericKey>, GenericRow, KeyValueStore<Bytes, byte[]>> materialized
        = buildWindowedTableMaterialized(
        source,
        buildContext,
        materializedFactory,
        keySerde,
        valueSerde
    );

    final KTable<Windowed<GenericKey>, GenericRow> ktable = buildWindowedKTable(
        source,
        buildContext,
        consumed,
        windowedKeyGenerator(source.getSourceSchema()),
        materialized,
        valueSerde,
        stateStoreName,
        planInfo
    );

    return KTableHolder.materialized(
        ktable,
        buildSchema(source, true),
        ExecutionKeyFactory.windowed(buildContext, windowInfo),
        MaterializationInfo.builder(stateStoreName, physicalSchema.logicalSchema())
    );
  }

  abstract Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> buildTableMaterialized(
      final SourceStep<KTableHolder<GenericKey>> source,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final Serde<GenericKey> keySerde,
      final Serde<GenericRow> valueSerde
  );

  abstract Materialized<Windowed<GenericKey>, GenericRow, KeyValueStore<Bytes, byte[]>>
  buildWindowedTableMaterialized(
      final SourceStep<KTableHolder<Windowed<GenericKey>>> source,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final Serde<Windowed<GenericKey>> keySerde,
      final Serde<GenericRow> valueSerde
  );

  abstract <K> KTable<K, GenericRow> buildKTable(
      final SourceStep<?> streamSource,
      final RuntimeBuildContext buildContext,
      final Consumed<K, GenericRow> consumed,
      final Function<K, Collection<?>> keyGenerator,
      final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized,
      final Serde<GenericRow> valueSerde,
      final String stateStoreName,
      final PlanInfo planInfo
  );

  abstract <K> KTable<K, GenericRow> buildWindowedKTable(
      final SourceStep<?> streamSource,
      final RuntimeBuildContext buildContext,
      final Consumed<K, GenericRow> consumed,
      final Function<K, Collection<?>> keyGenerator,
      final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized,
      final Serde<GenericRow> valueSerde,
      final String stateStoreName,
      final PlanInfo planInfo
  );

}
