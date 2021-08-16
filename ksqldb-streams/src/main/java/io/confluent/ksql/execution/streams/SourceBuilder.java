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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.serde.StaticTopicSerde.Callback;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public final class SourceBuilder extends SourceBuilderBase{

  private SourceBuilder() {
  }

  public static KTableHolder<GenericKey> buildTable(
      final RuntimeBuildContext buildContext,
      final TableSource source,
      final ConsumedFactory consumedFactory,
      final MaterializedFactory materializedFactory,
      final PlanInfo planInfo
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(buildContext, source, physicalSchema);

    final Serde<GenericKey> keySerde = buildContext.buildKeySerde(
        source.getFormats().getKeyFormat(),
        physicalSchema,
        source.getProperties().getQueryContext()
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
        buildSchema(source, false),
        ExecutionKeyFactory.unwindowed(buildContext),
        MaterializationInfo.builder(stateStoreName, physicalSchema.logicalSchema())
    );
  }

  static KTableHolder<Windowed<GenericKey>> buildWindowedTable(
      final RuntimeBuildContext buildContext,
      final WindowedTableSource source,
      final ConsumedFactory consumedFactory,
      final MaterializedFactory materializedFactory,
      final PlanInfo planInfo
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(buildContext, source, physicalSchema);

    final WindowInfo windowInfo = source.getWindowInfo();
    final Serde<Windowed<GenericKey>> keySerde = buildContext.buildKeySerde(
        source.getFormats().getKeyFormat(),
        windowInfo,
        physicalSchema,
        source.getProperties().getQueryContext()
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
        stateStoreName,
        source,
        buildContext,
        materializedFactory
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

  private static Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> buildTableMaterialized(
      final String stateStoreName,
      final TableSource source,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory
  ) {

    final PhysicalSchema physicalSchema = getPhysicalSchemaWithKeyAndPseudoCols(source);

    final QueryContext queryContext = QueryContext.Stacker.of(
        source.getProperties().getQueryContext())
        .push("Materialize").getQueryContext();

    final Serde<GenericRow> valueSerde = getValueSerdeWithAdditionalQueryContext(
        buildContext, source, physicalSchema, queryContext);

    final Serde<GenericKey> keySerde = buildContext.buildKeySerde(
        source.getFormats().getKeyFormat(),
        physicalSchema,
        queryContext
    );

    return materializedFactory.create(
        keySerde,
        valueSerde,
        stateStoreName
    );

  }

  private static Materialized<Windowed<GenericKey>, GenericRow, KeyValueStore<Bytes, byte[]>>
  buildWindowedTableMaterialized(
      final String stateStoreName,
      final WindowedTableSource source,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory
  ) {

    final PhysicalSchema physicalSchema = getPhysicalSchemaWithKeyAndPseudoCols(source);

    final QueryContext queryContext = QueryContext.Stacker.of(
        source.getProperties().getQueryContext())
        .push("Materialize").getQueryContext();

    final Serde<GenericRow> valueSerde = getValueSerdeWithAdditionalQueryContext(
        buildContext, source, physicalSchema, queryContext);

    final Serde<Windowed<GenericKey>> keySerde = buildContext.buildKeySerde(
        source.getFormats().getKeyFormat(),
        source.getWindowInfo(),
        physicalSchema,
        queryContext
    );

    return materializedFactory.create(
        keySerde,
        valueSerde,
        stateStoreName
    );
  }

  private static Serde<GenericRow> getValueSerdeWithAdditionalQueryContext(
      final RuntimeBuildContext buildContext,
      final SourceStep<?> streamSource,
      final PhysicalSchema physicalSchema,
      final QueryContext queryContext) {

    return buildContext.buildValueSerde(
        streamSource.getFormats().getValueFormat(),
        physicalSchema,
        queryContext
    );
  }

  private static PhysicalSchema getPhysicalSchemaWithKeyAndPseudoCols(
      final SourceStep<?> streamSource) {

    final boolean windowed = streamSource instanceof WindowedTableSource;

    final FormatInfo formatInfo = streamSource.getFormats().getKeyFormat();
    final SerdeFeatures serdeFeatures = streamSource.getFormats().getKeyFeatures();
    final KeyFormat keyFormat = windowed
        ? KeyFormat.windowed(
            formatInfo, serdeFeatures, ((WindowedTableSource) streamSource).getWindowInfo())
        : KeyFormat.nonWindowed(formatInfo, serdeFeatures);

    final Formats formats = of(keyFormat, streamSource.getFormats().getValueFormat());

    return PhysicalSchema.from(
        streamSource.getSourceSchema().withPseudoAndKeyColsInValue(
            windowed, streamSource.getPseudoColumnVersion()),
        formats.getKeyFeatures(),
        formats.getValueFeatures()
    );
  }

  //todo: put this logic into TableSource and WindowedTableSource
  private static Formats of(final KeyFormat keyFormat, final FormatInfo valueFormat) {

    return Formats.of(
        keyFormat.getFormatInfo(),
        valueFormat,
        keyFormat.getFeatures(),
        SerdeFeatures.of()
    );
  }

  private static <K> KTable<K, GenericRow> buildKTable(
      final SourceStep<?> streamSource,
      final RuntimeBuildContext buildContext,
      final Consumed<K, GenericRow> consumed,
      final Function<K, Collection<?>> keyGenerator,
      final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized,
      final Serde<GenericRow> valueSerde,
      final String stateStoreName,
      final PlanInfo planInfo
  ) {
    final boolean forceChangelog = streamSource instanceof TableSource
        && ((TableSource) streamSource).isForceChangelog();

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

      return table
          .transformValues(new AddKeyAndPseudoColumns<>(
              keyGenerator, streamSource.getPseudoColumnVersion()));

    } else {
      final KTable<K, GenericRow> source = buildContext
          .getStreamsBuilder()
          .table(streamSource.getTopicName(), consumed);

      final boolean forceMaterialization = !planInfo.isRepartitionedInPlan(streamSource);

      final KTable<K, GenericRow> transformed = source.transformValues(
          new AddKeyAndPseudoColumns<>(keyGenerator, streamSource.getPseudoColumnVersion()));

      if (forceMaterialization) {
        // add this identity mapValues call to prevent the source-changelog
        // optimization in kafka streams - we don't want this optimization to
        // be enabled because we cannot require symmetric serialization between
        // producer and KSQL (see https://issues.apache.org/jira/browse/KAFKA-10179
        // and https://github.com/confluentinc/ksql/issues/5673 for more details)
        table = transformed.mapValues(row -> row, materialized);
      } else {
        // if we know this table source is repartitioned later in the topology,
        // we do not need to force a materialization at this source step since the
        // re-partitioned topic will be used for any subsequent state stores, in lieu
        // of the original source topic, thus avoiding the issues above.
        // See https://github.com/confluentinc/ksql/issues/6650
        table = transformed.mapValues(row -> row);
      }
    }

    return table;
  }

}
