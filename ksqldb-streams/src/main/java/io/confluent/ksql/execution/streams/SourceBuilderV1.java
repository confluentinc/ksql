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

import static io.confluent.ksql.execution.streams.SourceBuilderUtils.AddKeyAndPseudoColumns;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.buildSchema;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.buildSourceConsumed;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.changelogTopic;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getKeySerde;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getPhysicalSchema;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getRegisterCallback;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getValueSerde;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getWindowedKeySerde;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.tableChangeLogOpName;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.windowedKeyGenerator;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSourceV1;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.SourceBuilderUtils.AddKeyAndPseudoColumns.AddKeyAndPseudoColumnsProcessor;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.serde.StaticTopicSerde.Callback;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlException;
import java.util.Collection;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

final class SourceBuilderV1 extends SourceBuilderBase {

  private static final SourceBuilderV1 instance;

  static {
    instance = new SourceBuilderV1();
  }

  private SourceBuilderV1() {
  }

  public static SourceBuilderV1 instance() {
    return instance;
  }

  public KStreamHolder<GenericKey> buildStream(
      final RuntimeBuildContext buildContext,
      final StreamSource source,
      final ConsumedFactory consumedFactory
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
        AutoOffsetReset.LATEST,
        buildContext,
        consumedFactory
    );

    final KStream<GenericKey, GenericRow> kstream = buildKStream(
        source,
        buildContext,
        consumed,
        nonWindowedKeyGenerator(source.getSourceSchema())
    );

    return new KStreamHolder<>(
        kstream,
        buildSchema(source, false),
        ExecutionKeyFactory.unwindowed(buildContext)
    );
  }

  public KStreamHolder<Windowed<GenericKey>> buildWindowedStream(
      final RuntimeBuildContext buildContext,
      final WindowedStreamSource source,
      final ConsumedFactory consumedFactory
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(buildContext, source, physicalSchema);

    final WindowInfo windowInfo = source.getWindowInfo();
    final Serde<Windowed<GenericKey>> keySerde = getWindowedKeySerde(
        source,
        physicalSchema,
        buildContext,
        windowInfo
    );

    final Consumed<Windowed<GenericKey>, GenericRow> consumed = buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.LATEST,
        buildContext,
        consumedFactory
    );

    final KStream<Windowed<GenericKey>, GenericRow> kstream = buildKStream(
        source,
        buildContext,
        consumed,
        windowedKeyGenerator(source.getSourceSchema())
    );

    return new KStreamHolder<>(
        kstream,
        buildSchema(source, true),
        ExecutionKeyFactory.windowed(buildContext, windowInfo)
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

    if (source instanceof WindowedTableSource) {
      windowInfo = ((WindowedTableSource) source).getWindowInfo();
    } else {
      throw new IllegalArgumentException("Expected a version of WindowedTableSource");
    }

    final Serde<Windowed<GenericKey>> keySerde = SourceBuilderUtils.getWindowedKeySerde(
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
        = materializedFactory.create(
        keySerde,
        valueSerde,
        stateStoreName
    );

    final KTable<Windowed<GenericKey>, GenericRow> ktable = buildKTable(
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

  @Override
  public Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> buildTableMaterialized(
      final SourceStep<KTableHolder<GenericKey>> source,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final Serde<GenericKey> keySerde,
      final Serde<GenericRow> valueSerde,
      final String stateStoreName
  ) {
    return materializedFactory.create(
        keySerde,
        valueSerde,
        stateStoreName
    );
  }

  @Override
  <K> KTable<K, GenericRow> buildKTable(
      final SourceStep<?> streamSource,
      final RuntimeBuildContext buildContext,
      final Consumed<K, GenericRow> consumed,
      final Function<K, Collection<?>> keyGenerator,
      final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized,
      final Serde<GenericRow> valueSerde,
      final String stateStoreName,
      final PlanInfo planInfo
  ) {

    validateNotUsingOldExecutionStepWithNewQueries(streamSource);

    final boolean forceChangelog = streamSource instanceof TableSourceV1
        && ((TableSourceV1) streamSource).isForceChangelog();

    final KTable<K, GenericRow> table;
    if (!forceChangelog) {
      final String changelogTopic = changelogTopic(buildContext, stateStoreName);
      final Callback onFailure = getRegisterCallback(
          buildContext, streamSource.getFormats().getValueFormat());

      table = buildContext
          .getStreamsBuilder()
          .table(
              streamSource.getTopicName(),
              consumed.withValueSerde(StaticTopicSerde.wrap(changelogTopic, valueSerde, onFailure)),
              materialized
          );
    } else {
      final KTable<K, GenericRow> source = buildContext
          .getStreamsBuilder()
          .table(streamSource.getTopicName(), consumed);

      final boolean forceMaterialization = !planInfo.isRepartitionedInPlan(streamSource);
      if (forceMaterialization) {
        // add this identity mapValues call to prevent the source-changelog
        // optimization in kafka streams - we don't want this optimization to
        // be enabled because we cannot require symmetric serialization between
        // producer and KSQL (see https://issues.apache.org/jira/browse/KAFKA-10179
        // and https://github.com/confluentinc/ksql/issues/5673 for more details)
        table = source.mapValues(row -> row, materialized);
      } else {
        // if we know this table source is repartitioned later in the topology,
        // we do not need to force a materialization at this source step since the
        // re-partitioned topic will be used for any subsequent state stores, in lieu
        // of the original source topic, thus avoiding the issues above.
        // See https://github.com/confluentinc/ksql/issues/6650
        table = source.mapValues(row -> row);
      }
    }

    return table
        .transformValues(new AddKeyAndPseudoColumns<>(
            keyGenerator,
            streamSource.getPseudoColumnVersion(),
            streamSource.getSourceSchema().headers()));
  }

  private <K> KStream<K, GenericRow> buildKStream(
      final SourceStep<?> streamSource,
      final RuntimeBuildContext buildContext,
      final Consumed<K, GenericRow> consumed,
      final Function<K, Collection<?>> keyGenerator
  ) {
    final KStream<K, GenericRow> stream = buildContext.getStreamsBuilder()
        .stream(streamSource.getTopicName(), consumed);

    final int pseudoColumnVersion = streamSource.getPseudoColumnVersion();
    return stream.processValues(() -> new AddKeyAndPseudoColumnsProcessor<>(
            keyGenerator, pseudoColumnVersion, streamSource.getSourceSchema().headers()));
  }

  private static Function<GenericKey, Collection<?>> nonWindowedKeyGenerator(
      final LogicalSchema schema
  ) {
    final GenericKey nullKey = GenericKey.builder(schema).appendNulls().build();
    return key -> key == null ? nullKey.values() : key.values();
  }

  private static void validateNotUsingOldExecutionStepWithNewQueries(
      final SourceStep<?> streamSource) {

    if (streamSource instanceof TableSourceV1 && streamSource.getPseudoColumnVersion() != 0) {
      throw new KsqlException("Should not be using old execution step version with new queries");
    }
  }

}
