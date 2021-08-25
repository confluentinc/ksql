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
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.addMaterializedContext;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.buildKeySerde;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.buildSchema;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.buildWindowedKeySerde;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.changelogTopic;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getRegisterCallback;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getValueSerde;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.serde.StaticTopicSerde.Callback;
import java.util.Collection;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

final class SourceBuilder extends SourceBuilderBase {

  private static final SourceBuilder instance;

  static {
    instance = new SourceBuilder();
  }

  private SourceBuilder() {
  }

  public static SourceBuilder instance() {
    return instance;
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
      return transformed.mapValues(row -> row, materialized);

    } else {
      // if we know this table source is repartitioned later in the topology,
      // we do not need to force a materialization at this source step since the
      // re-partitioned topic will be used for any subsequent state stores, in lieu
      // of the original source topic, thus avoiding the issues above.
      // See https://github.com/confluentinc/ksql/issues/6650
      return transformed.mapValues(row -> row);
    }

  }

  @Override
  <K> KTable<K, GenericRow> buildWindowedKTable(SourceStep<?> streamSource,
      final RuntimeBuildContext buildContext,
      final Consumed<K, GenericRow> consumed,
      final Function<K, Collection<?>> keyGenerator,
      final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized,
      final Serde<GenericRow> valueSerde,
      final String stateStoreName,
      final PlanInfo planInfo
  ) {
    final String changelogTopic = changelogTopic(buildContext, stateStoreName);
    final Callback onFailure = getRegisterCallback(
        buildContext, streamSource.getFormats().getValueFormat());

    final KTable<K, GenericRow> source = buildContext
        .getStreamsBuilder()
        .table(
            streamSource.getTopicName(),
            consumed.withValueSerde(StaticTopicSerde.wrap(changelogTopic, valueSerde, onFailure))
        );

    return source.transformValues(new AddKeyAndPseudoColumns<>(
        keyGenerator, streamSource.getPseudoColumnVersion()), materialized);
  }

  @Override
  Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> buildTableMaterialized(
      final SourceStep<KTableHolder<GenericKey>> source,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final Serde<GenericKey> sourceKeySerde,
      final Serde<GenericRow> sourceValueSerde
  ) {

    final String stateStoreName = SourceBuilderUtils.tableChangeLogOpName(source.getProperties());

    final PhysicalSchema physicalSchema = getPhysicalSchemaWithKeyAndPseudoCols(source);

    final QueryContext queryContext = addMaterializedContext(source);

    final Serde<GenericRow> valueSerdeToMaterialize = getValueSerde(
        buildContext,
        source,
        physicalSchema,
        queryContext
    );

    final Serde<GenericKey> keySerdeToMaterialize = buildKeySerde(
        source,
        physicalSchema,
        buildContext,
        queryContext
    );

    return materializedFactory.create(
        keySerdeToMaterialize,
        valueSerdeToMaterialize,
        stateStoreName
    );
  }

  @Override
  Materialized<Windowed<GenericKey>, GenericRow, KeyValueStore<Bytes, byte[]>>
      buildWindowedTableMaterialized(
      final SourceStep<KTableHolder<Windowed<GenericKey>>> source,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final Serde<Windowed<GenericKey>> sourceKeySerde,
      final Serde<GenericRow> sourceValueSerde
  ) {

    final String stateStoreName = SourceBuilderUtils.tableChangeLogOpName(source.getProperties());

    final PhysicalSchema physicalSchema = getPhysicalSchemaWithKeyAndPseudoCols(source);

    final QueryContext queryContext = addMaterializedContext(source);

    final Serde<GenericRow> valueSerdeToMaterialize = getValueSerde(
        buildContext,
        source,
        physicalSchema,
        queryContext
    );

    final Serde<Windowed<GenericKey>> keySerdeToMaterialize = buildWindowedKeySerde(
        source,
        physicalSchema,
        buildContext,
        ((WindowedTableSource) source).getWindowInfo(),
        queryContext
    );

    return materializedFactory.create(
        keySerdeToMaterialize,
        valueSerdeToMaterialize,
        stateStoreName
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
        buildSchema(streamSource, windowed),
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

}
