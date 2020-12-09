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

import static io.confluent.ksql.util.KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SERVICE_ID_CONFIG;
import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicyFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.serde.StaticTopicSerde.Callback;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;

public final class SourceBuilder {

  private static final Collection<?> NULL_WINDOWED_KEY_COLUMNS = Collections.unmodifiableList(
      Arrays.asList(null, null, null)
  );

  private SourceBuilder() {
  }

  public static KStreamHolder<GenericKey> buildStream(
      final KsqlQueryBuilder queryBuilder,
      final StreamSource source,
      final ConsumedFactory consumedFactory
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, source, physicalSchema);

    final Serde<GenericKey> keySerde = queryBuilder.buildKeySerde(
        source.getFormats().getKeyFormat(),
        physicalSchema,
        source.getProperties().getQueryContext()
    );

    final Consumed<GenericKey, GenericRow> consumed = buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.LATEST,
        queryBuilder,
        consumedFactory
    );

    final KStream<GenericKey, GenericRow> kstream = buildKStream(
        source,
        queryBuilder,
        consumed,
        nonWindowedKeyGenerator(source.getSourceSchema())
    );

    return new KStreamHolder<>(
        kstream,
        buildSchema(source, false),
        ExecutionKeyFactory.unwindowed(queryBuilder)
    );
  }

  static KStreamHolder<Windowed<GenericKey>> buildWindowedStream(
      final KsqlQueryBuilder queryBuilder,
      final WindowedStreamSource source,
      final ConsumedFactory consumedFactory
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, source, physicalSchema);

    final WindowInfo windowInfo = source.getWindowInfo();
    final Serde<Windowed<GenericKey>> keySerde = queryBuilder.buildKeySerde(
        source.getFormats().getKeyFormat(),
        windowInfo,
        physicalSchema,
        source.getProperties().getQueryContext()
    );

    final Consumed<Windowed<GenericKey>, GenericRow> consumed = buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.LATEST,
        queryBuilder,
        consumedFactory
    );

    final KStream<Windowed<GenericKey>, GenericRow> kstream = buildKStream(
        source,
        queryBuilder,
        consumed,
        windowedKeyGenerator(source.getSourceSchema())
    );

    return new KStreamHolder<>(
        kstream,
        buildSchema(source, true),
        ExecutionKeyFactory.windowed(queryBuilder, windowInfo)
    );
  }

  public static KTableHolder<GenericKey> buildTable(
      final KsqlQueryBuilder queryBuilder,
      final TableSource source,
      final ConsumedFactory consumedFactory,
      final MaterializedFactory materializedFactory,
      final PlanInfo planInfo
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, source, physicalSchema);

    final Serde<GenericKey> keySerde = queryBuilder.buildKeySerde(
        source.getFormats().getKeyFormat(),
        physicalSchema,
        source.getProperties().getQueryContext()
    );

    final Consumed<GenericKey, GenericRow> consumed = buildSourceConsumed(
        source,
        keySerde,
        valueSerde,
        AutoOffsetReset.EARLIEST,
        queryBuilder,
        consumedFactory
    );

    final String stateStoreName = tableChangeLogOpName(source.getProperties());
    final Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        materializedFactory.create(
            keySerde,
            valueSerde,
            stateStoreName
        );

    final KTable<GenericKey, GenericRow> ktable = buildKTable(
        source,
        queryBuilder,
        consumed,
        GenericKey::values,
        materialized,
        valueSerde,
        stateStoreName,
        planInfo
    );

    return KTableHolder.unmaterialized(
        ktable,
        buildSchema(source, false),
        ExecutionKeyFactory.unwindowed(queryBuilder)
    );
  }

  static KTableHolder<Windowed<GenericKey>> buildWindowedTable(
      final KsqlQueryBuilder queryBuilder,
      final WindowedTableSource source,
      final ConsumedFactory consumedFactory,
      final MaterializedFactory materializedFactory,
      final PlanInfo planInfo
  ) {
    final PhysicalSchema physicalSchema = getPhysicalSchema(source);

    final Serde<GenericRow> valueSerde = getValueSerde(queryBuilder, source, physicalSchema);

    final WindowInfo windowInfo = source.getWindowInfo();
    final Serde<Windowed<GenericKey>> keySerde = queryBuilder.buildKeySerde(
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
        queryBuilder,
        consumedFactory
    );

    final String stateStoreName = tableChangeLogOpName(source.getProperties());
    final Materialized<Windowed<GenericKey>, GenericRow, KeyValueStore<Bytes, byte[]>>
        materialized = materializedFactory.create(
        keySerde,
        valueSerde,
        stateStoreName
    );

    final KTable<Windowed<GenericKey>, GenericRow> ktable = buildKTable(
        source,
        queryBuilder,
        consumed,
        windowedKeyGenerator(source.getSourceSchema()),
        materialized,
        valueSerde,
        stateStoreName,
        planInfo
    );

    return KTableHolder.unmaterialized(
        ktable,
        buildSchema(source, true),
        ExecutionKeyFactory.windowed(queryBuilder, windowInfo)
    );
  }

  private static LogicalSchema buildSchema(
      final SourceStep<?> source,
      final boolean windowed
  ) {
    return source
        .getSourceSchema()
        .withPseudoAndKeyColsInValue(windowed);
  }

  private static Serde<GenericRow> getValueSerde(
      final KsqlQueryBuilder queryBuilder,
      final SourceStep<?> streamSource,
      final PhysicalSchema physicalSchema) {
    return queryBuilder.buildValueSerde(
        streamSource.getFormats().getValueFormat(),
        physicalSchema,
        streamSource.getProperties().getQueryContext()
    );
  }

  private static PhysicalSchema getPhysicalSchema(final SourceStep<?> streamSource) {
    return PhysicalSchema.from(
        streamSource.getSourceSchema(),
        streamSource.getFormats().getKeyFeatures(),
        streamSource.getFormats().getValueFeatures()
    );
  }

  private static <K> KStream<K, GenericRow> buildKStream(
      final SourceStep<?> streamSource,
      final KsqlQueryBuilder queryBuilder,
      final Consumed<K, GenericRow> consumed,
      final Function<K, Collection<?>> keyGenerator
  ) {
    final KStream<K, GenericRow> stream = queryBuilder.getStreamsBuilder()
        .stream(streamSource.getTopicName(), consumed);

    return stream
        .transformValues(new AddKeyAndTimestampColumns<>(keyGenerator));
  }

  private static <K> KTable<K, GenericRow> buildKTable(
      final SourceStep<?> streamSource,
      final KsqlQueryBuilder queryBuilder,
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
      final String changelogTopic = changelogTopic(queryBuilder, stateStoreName);
      final Callback onFailure = getRegisterCallback(
          queryBuilder, streamSource.getFormats().getValueFormat());

      table = queryBuilder
          .getStreamsBuilder()
          .table(
              streamSource.getTopicName(),
              consumed.withValueSerde(StaticTopicSerde.wrap(changelogTopic, valueSerde, onFailure)),
              materialized
          );
    } else {
      final KTable<K, GenericRow> source = queryBuilder
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
        .transformValues(new AddKeyAndTimestampColumns<>(keyGenerator));
  }

  private static StaticTopicSerde.Callback getRegisterCallback(
      final KsqlQueryBuilder builder,
      final FormatInfo valueFormat
  ) {
    final boolean schemaRegistryEnabled = !builder
        .getKsqlConfig()
        .getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
        .isEmpty();

    final boolean useSR = FormatFactory
        .fromName(valueFormat.getFormat())
        .supportsFeature(SerdeFeature.SCHEMA_INFERENCE);

    if (!schemaRegistryEnabled || !useSR) {
      return (t1, t2, data) -> { };
    }

    return new RegisterSchemaCallback(builder.getServiceContext().getSchemaRegistryClient());
  }

  /**
   * This code mirrors the logic that generates the name for changelog topics
   * in kafka streams, which follows the pattern:
   * <pre>
   *    applicationID + "-" + stateStoreName + "-changelog".
   * </pre>
   */
  private static String changelogTopic(
      final KsqlQueryBuilder queryBuilder,
      final String stateStoreName
  ) {
    return ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX
        + queryBuilder.getKsqlConfig().getString(KSQL_SERVICE_ID_CONFIG)
        + queryBuilder.getKsqlConfig().getString(KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG)
        + queryBuilder.getQueryId().toString()
        + "-"
        + stateStoreName
        + "-changelog";
  }

  private static TimestampExtractor timestampExtractor(
      final KsqlConfig ksqlConfig,
      final LogicalSchema sourceSchema,
      final Optional<TimestampColumn> timestampColumn,
      final SourceStep<?> streamSource,
      final KsqlQueryBuilder queryBuilder
  ) {
    final TimestampExtractionPolicy timestampPolicy = TimestampExtractionPolicyFactory.create(
        ksqlConfig,
        sourceSchema,
        timestampColumn
    );

    final Optional<Column> tsColumn = timestampColumn.map(TimestampColumn::getColumn)
        .map(c -> sourceSchema.findColumn(c).orElseThrow(IllegalStateException::new));

    final QueryContext queryContext = streamSource.getProperties().getQueryContext();

    return timestampPolicy.create(
        tsColumn,
        ksqlConfig.getBoolean(KsqlConfig.KSQL_TIMESTAMP_THROW_ON_INVALID),
        queryBuilder.getProcessingLogger(queryContext)
    );
  }

  private static <K> Consumed<K, GenericRow> buildSourceConsumed(
      final SourceStep<?> streamSource,
      final Serde<K> keySerde,
      final Serde<GenericRow> valueSerde,
      final Topology.AutoOffsetReset defaultReset,
      final KsqlQueryBuilder queryBuilder,
      final ConsumedFactory consumedFactory) {
    final TimestampExtractor timestampExtractor = timestampExtractor(
        queryBuilder.getKsqlConfig(),
        streamSource.getSourceSchema(),
        streamSource.getTimestampColumn(),
        streamSource,
        queryBuilder
    );
    final Consumed<K, GenericRow> consumed = consumedFactory
        .create(keySerde, valueSerde)
        .withTimestampExtractor(timestampExtractor);
    return consumed.withOffsetResetPolicy(getAutoOffsetReset(defaultReset, queryBuilder));
  }

  private static String tableChangeLogOpName(final ExecutionStepPropertiesV1 props) {
    final List<String> parts = props.getQueryContext().getContext();
    Stacker stacker = new Stacker();
    for (final String part : parts.subList(0, parts.size() - 1)) {
      stacker = stacker.push(part);
    }
    return StreamsUtil.buildOpName(stacker.push("Reduce").getQueryContext());
  }

  private static Function<Windowed<GenericKey>, Collection<?>> windowedKeyGenerator(
      final LogicalSchema schema
  ) {
    if (schema.key().isEmpty()) {
      throw new IllegalStateException("Windowed sources require a key column");
    }

    return windowedKey -> {
      if (windowedKey == null) {
        return NULL_WINDOWED_KEY_COLUMNS;
      }

      final Window window = windowedKey.window();
      final GenericKey key = windowedKey.key();

      final List<Object> keys = new ArrayList<>(schema.key().size() + 2);
      keys.addAll(key.values());
      keys.add(window.start());
      keys.add(window.end());
      return Collections.unmodifiableCollection(keys);
    };
  }

  private static Function<GenericKey, Collection<?>> nonWindowedKeyGenerator(
      final LogicalSchema schema
  ) {
    final GenericKey nullKey = GenericKey.builder(schema).appendNulls().build();
    return key -> key == null ? nullKey.values() : key.values();
  }

  private static class AddKeyAndTimestampColumns<K>
      implements ValueTransformerWithKeySupplier<K, GenericRow, GenericRow> {

    private final Function<K, Collection<?>> keyGenerator;

    AddKeyAndTimestampColumns(final Function<K, Collection<?>> keyGenerator) {
      this.keyGenerator = requireNonNull(keyGenerator, "keyGenerator");
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

          final long timestamp = processorContext.timestamp();
          final Collection<?> keyColumns = keyGenerator.apply(key);

          row.ensureAdditionalCapacity(1 + keyColumns.size());
          row.append(timestamp);
          row.appendAll(keyColumns);
          return row;
        }

        @Override
        public void close() {
        }
      };
    }
  }

  private static Topology.AutoOffsetReset getAutoOffsetReset(
      final Topology.AutoOffsetReset defaultValue,
      final KsqlQueryBuilder queryBuilder) {
    final Object offestReset = queryBuilder.getKsqlConfig()
        .getKsqlStreamConfigProps()
        .get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    if (offestReset == null) {
      return defaultValue;
    }

    try {
      return AutoOffsetReset.valueOf(offestReset.toString().toUpperCase());
    } catch (final Exception e) {
      throw new ConfigException(
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          offestReset,
          "Unknown value"
      );
    }
  }
}
