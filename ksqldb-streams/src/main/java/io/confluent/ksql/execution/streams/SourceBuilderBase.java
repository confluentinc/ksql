package io.confluent.ksql.execution.streams;

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
import io.confluent.ksql.util.KsqlException;
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
    final PhysicalSchema physicalSchema = SourceBuilderUtils.getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = SourceBuilderUtils.getValueSerde(buildContext, source, physicalSchema);

    final Serde<GenericKey> keySerde = buildContext.buildKeySerde(
        source.getFormats().getKeyFormat(),
        physicalSchema,
        source.getProperties().getQueryContext()
    );

    final Consumed<GenericKey, GenericRow> consumed = SourceBuilderUtils.buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.EARLIEST,
        buildContext,
        consumedFactory
    );

    final String stateStoreName = SourceBuilderUtils.tableChangeLogOpName(source.getProperties());

    final Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> materialized
        = buildTableMaterialized(stateStoreName, source, buildContext, materializedFactory);

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
        SourceBuilderUtils.buildSchema(source, false),
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
    final PhysicalSchema physicalSchema = SourceBuilderUtils.getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = SourceBuilderUtils.getValueSerde(buildContext, source, physicalSchema);

    final WindowInfo windowInfo;

    if (source instanceof WindowedTableSourceV1) {
      windowInfo = ((WindowedTableSourceV1) source).getWindowInfo();
    } else if (source instanceof WindowedTableSource) {
      windowInfo = ((WindowedTableSource) source).getWindowInfo();
    } else {
      throw new KsqlException("Expected a version of WindowedTableSource");
    }

    final Serde<Windowed<GenericKey>> keySerde = buildContext.buildKeySerde(
        source.getFormats().getKeyFormat(),
        windowInfo,
        physicalSchema,
        source.getProperties().getQueryContext()
    );

    final Consumed<Windowed<GenericKey>, GenericRow> consumed = SourceBuilderUtils.buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.EARLIEST,
        buildContext,
        consumedFactory
    );

    final String stateStoreName = SourceBuilderUtils.tableChangeLogOpName(source.getProperties());

    final Materialized<Windowed<GenericKey>, GenericRow, KeyValueStore<Bytes, byte[]>> materialized
        = buildWindowedTableMaterialized(
        stateStoreName,
        source,
        buildContext,
        materializedFactory
    );

    final KTable<Windowed<GenericKey>, GenericRow> ktable = buildWindowedKTable(
        source,
        buildContext,
        consumed,
        SourceBuilderUtils.windowedKeyGenerator(source.getSourceSchema()),
        materialized,
        valueSerde,
        stateStoreName,
        planInfo
    );

    return KTableHolder.materialized(
        ktable,
        SourceBuilderUtils.buildSchema(source, true),
        ExecutionKeyFactory.windowed(buildContext, windowInfo),
        MaterializationInfo.builder(stateStoreName, physicalSchema.logicalSchema())
    );
  }

  abstract Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> buildTableMaterialized(
      final String stateStoreName,
      final SourceStep<KTableHolder<GenericKey>> source,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory
  );

  abstract Materialized<Windowed<GenericKey>, GenericRow, KeyValueStore<Bytes, byte[]>>
  buildWindowedTableMaterialized(
      final String stateStoreName,
      final SourceStep<KTableHolder<Windowed<GenericKey>>> source,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory
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
