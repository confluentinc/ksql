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

import static io.confluent.ksql.execution.streams.SourceBuilderUtils.AddKeyAndPseudoColumns;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.addMaterializedContext;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.buildSchema;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.changelogTopic;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getKeySerde;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getRegisterCallback;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getValueSerde;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getWindowedKeySerde;
import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.serde.StaticTopicSerde.Callback;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
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

    final KTable<K, GenericRow> maybeMaterialized;

    if (forceMaterialization) {
      // besides materializing necessary pseudocolumns, we also materialize to prevent the
      // source-changelog optimization in kafka streams - we don't want this optimization to
      // be enabled because we cannot require symmetric serialization between
      // producer and KSQL (see https://issues.apache.org/jira/browse/KAFKA-10179
      // and https://github.com/confluentinc/ksql/issues/5673 for more details)
      maybeMaterialized = source.transformValues(
          new AddPseudoColumnsToMaterialize<>(streamSource.getPseudoColumnVersion()), materialized);
    } else {
      // if we know this table source is repartitioned later in the topology,
      // we do not need to force a materialization at this source step since the
      // re-partitioned topic will be used for any subsequent state stores, in lieu
      // of the original source topic, thus avoiding the issues above.
      // See https://github.com/confluentinc/ksql/issues/6650
      maybeMaterialized = source.transformValues(
          new AddPseudoColumnsToMaterialize<>(streamSource.getPseudoColumnVersion()));
    }

    return maybeMaterialized.transformValues(new AddRemainingPseudoAndKeyCols<>(
        keyGenerator,
        streamSource.getPseudoColumnVersion()));
  }

  @Override
  <K> KTable<K, GenericRow> buildWindowedKTable(
      final SourceStep<?> streamSource,
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
      final Serde<GenericRow> sourceValueSerde,
      final String stateStoreName
  ) {

    final PhysicalSchema physicalSchema = getPhysicalSchemaWithPseudoColumnsToMaterialize(source);

    final QueryContext queryContext = addMaterializedContext(source);

    final Serde<GenericRow> valueSerdeToMaterialize = getValueSerde(
        buildContext,
        source,
        physicalSchema,
        queryContext
    );

    final Serde<GenericKey> keySerdeToMaterialize = getKeySerde(
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
      final Serde<GenericRow> sourceValueSerde,
      final String stateStoreName
  ) {

    final PhysicalSchema physicalSchema = getPhysicalSchemaWithPseudoColumnsToMaterialize(source);

    final QueryContext queryContext = addMaterializedContext(source);

    final Serde<GenericRow> valueSerdeToMaterialize = getValueSerde(
        buildContext,
        source,
        physicalSchema,
        queryContext
    );

    final Serde<Windowed<GenericKey>> keySerdeToMaterialize = getWindowedKeySerde(
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

  private static PhysicalSchema getPhysicalSchemaWithPseudoColumnsToMaterialize(
      final SourceStep<?> streamSource) {

    final boolean windowed = streamSource instanceof WindowedTableSource;

    final FormatInfo formatInfo = streamSource.getFormats().getKeyFormat();
    final SerdeFeatures serdeFeatures = streamSource.getFormats().getKeyFeatures();

    final KeyFormat keyFormat = windowed
        ? KeyFormat.windowed(
        formatInfo, serdeFeatures, ((WindowedTableSource) streamSource).getWindowInfo())
        : KeyFormat.nonWindowed(formatInfo, serdeFeatures);

    final Formats format = of(keyFormat, streamSource.getFormats().getValueFormat());

    //we currently don't materialize partition and offset to get them downstream for windowed
    //tables, so act accordingly here
    final LogicalSchema withPseudoCols =
        windowed
            ? buildSchema(streamSource, true)
            : streamSource.getSourceSchema().withPseudoColumnsToMaterialize(
                streamSource.getPseudoColumnVersion());

    return PhysicalSchema.from(
        withPseudoCols,
        format.getKeyFeatures(),
        format.getValueFeatures()
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

  private static class AddPseudoColumnsToMaterialize<K>
      implements ValueTransformerWithKeySupplier<K, GenericRow, GenericRow> {

    private final int pseudoColumnVersion;

    AddPseudoColumnsToMaterialize(final int pseudoColumnVersion) {
      this.pseudoColumnVersion = pseudoColumnVersion;
    }

    @Override
    public ValueTransformerWithKey<K, GenericRow, GenericRow> get() {
      return new ValueTransformerWithKey<K, GenericRow, GenericRow>() {
        private ProcessorContext processorContext;

        @Override
        public void init(final ProcessorContext processorContext) {
          this.processorContext = requireNonNull(processorContext, "processorContext");
        }

        @Override
        public GenericRow transform(final K key, final GenericRow row) {
          if (row == null) {
            return row;
          }

          final int additionalCapacity = (int) SystemColumns.pseudoColumnNames(pseudoColumnVersion)
              .stream()
              .filter(SystemColumns::mustBeMaterializedForTableJoins)
              .count();

          row.ensureAdditionalCapacity(additionalCapacity);

          if (pseudoColumnVersion >= SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION) {
            final int partition = processorContext.partition();
            final long offset = processorContext.offset();
            row.append(partition);
            row.append(offset);
          }

          return row;
        }

        @Override
        public void close() {
        }
      };
    }
  }

  private static class AddRemainingPseudoAndKeyCols<K>
      implements ValueTransformerWithKeySupplier<K, GenericRow, GenericRow> {

    private final Function<K, Collection<?>> keyGenerator;
    private final int pseudoColumnVersion;

    AddRemainingPseudoAndKeyCols(
        final Function<K, Collection<?>> keyGenerator, final int pseudoColumnVersion) {
      this.keyGenerator = requireNonNull(keyGenerator, "keyGenerator");
      this.pseudoColumnVersion = pseudoColumnVersion;
    }

    @Override
    public ValueTransformerWithKey<K, GenericRow, GenericRow> get() {
      return new ValueTransformerWithKey<K, GenericRow, GenericRow>() {
        private ProcessorContext processorContext;

        @Override
        public void init(final ProcessorContext processorContext) {
          this.processorContext = requireNonNull(processorContext, "processorContext");
        }

        @Override
        public GenericRow transform(final K key, final GenericRow row) {
          if (row == null) {
            return row;
          }

          final Collection<?> keyColumns = keyGenerator.apply(key);

          //remove pseudocolumns we previously materialized so we can add them back in correct order

          //ensure extra capacity equal to number of pseudoColumns which we haven't materialized
          final int pseudoColumnsToAdd = (int) SystemColumns.pseudoColumnNames(pseudoColumnVersion)
              .stream()
              .filter(col -> !SystemColumns.mustBeMaterializedForTableJoins(col))
              .count();

          row.ensureAdditionalCapacity(pseudoColumnsToAdd);

          //append nulls, so we can populate the row with set() later without going OOB
          for (int i = 0; i < pseudoColumnsToAdd; i++) {
            row.append(null);
          }

          //calculate number of user columns, pseudo columns, and columns to shift for next steps
          final Set<ColumnName> columnNames = SystemColumns.pseudoColumnNames(pseudoColumnVersion);
          final int totalPseudoColumns = columnNames.size();
          final int pseudoColumnsToShift = totalPseudoColumns - pseudoColumnsToAdd;
          final int numUserColumns = row.size() - totalPseudoColumns;

          //create a list of pseudocolumns, in the order which they should be added
          final List<Object> pseudoCols = new ArrayList<>();
          if (pseudoColumnVersion >= SystemColumns.ROWTIME_PSEUDOCOLUMN_VERSION) {
            pseudoCols.add(processorContext.timestamp());
          }

          for (int i = numUserColumns; i < numUserColumns + pseudoColumnsToShift; i++) {
            pseudoCols.add(row.get(i));
          }

          for (int i = 0; i < totalPseudoColumns; i++) {
            row.set(i + numUserColumns, pseudoCols.get(i));
          }

          row.appendAll(keyColumns);
          return row;
        }

        @Override
        public void close() {
        }
      };
    }
  }

}
