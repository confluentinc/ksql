package io.confluent.ksql.execution.streams;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicyFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.util.KsqlConfig;
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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class SourceBuilderUtils {

  protected static final Collection<?> NULL_WINDOWED_KEY_COLUMNS = Collections.unmodifiableList(
      Arrays.asList(null, null, null)
  );

  private SourceBuilderUtils() {
  }

  protected static LogicalSchema buildSchema(
      final SourceStep<?> source,
      final boolean windowed
  ) {
    return source
        .getSourceSchema()
        .withPseudoAndKeyColsInValue(windowed, source.getPseudoColumnVersion());
  }

  protected static Serde<GenericRow> getValueSerde(
      final RuntimeBuildContext buildContext,
      final SourceStep<?> streamSource,
      final PhysicalSchema physicalSchema) {
    return buildContext.buildValueSerde(
        streamSource.getFormats().getValueFormat(),
        physicalSchema,
        streamSource.getProperties().getQueryContext()
    );
  }

  protected static PhysicalSchema getPhysicalSchema(final SourceStep<?> streamSource) {
    return PhysicalSchema.from(
        streamSource.getSourceSchema(),
        streamSource.getFormats().getKeyFeatures(),
        streamSource.getFormats().getValueFeatures()
    );
  }

  protected static StaticTopicSerde.Callback getRegisterCallback(
      final RuntimeBuildContext buildContext,
      final FormatInfo valueFormat
  ) {
    final boolean schemaRegistryEnabled = !buildContext
        .getKsqlConfig()
        .getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)
        .isEmpty();

    final boolean useSR = FormatFactory
        .fromName(valueFormat.getFormat())
        .supportsFeature(SerdeFeature.SCHEMA_INFERENCE);

    if (!schemaRegistryEnabled || !useSR) {
      return (t1, t2, data) -> { };
    }

    return new RegisterSchemaCallback(buildContext.getServiceContext().getSchemaRegistryClient());
  }

  /**
   * This code mirrors the logic that generates the name for changelog topics
   * in kafka streams, which follows the pattern:
   * <pre>
   *    applicationID + "-" + stateStoreName + "-changelog".
   * </pre>
   */
  protected static String changelogTopic(
      final RuntimeBuildContext buildContext,
      final String stateStoreName
  ) {
    return buildContext.getApplicationId()
        + "-"
        + stateStoreName
        + "-changelog";
  }


  protected static TimestampExtractor timestampExtractor(
      final KsqlConfig ksqlConfig,
      final LogicalSchema sourceSchema,
      final Optional<TimestampColumn> timestampColumn,
      final SourceStep<?> streamSource,
      final RuntimeBuildContext buildContext
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
        buildContext.getProcessingLogger(queryContext)
    );
  }

  protected static <K> Consumed<K, GenericRow> buildSourceConsumed(
      final SourceStep<?> streamSource,
      final Serde<K> keySerde,
      final Serde<GenericRow> valueSerde,
      final Topology.AutoOffsetReset defaultReset,
      final RuntimeBuildContext buildContext,
      final ConsumedFactory consumedFactory) {
    final TimestampExtractor timestampExtractor = timestampExtractor(
        buildContext.getKsqlConfig(),
        streamSource.getSourceSchema(),
        streamSource.getTimestampColumn(),
        streamSource,
        buildContext
    );
    final Consumed<K, GenericRow> consumed = consumedFactory
        .create(keySerde, valueSerde)
        .withTimestampExtractor(timestampExtractor);
    return consumed.withOffsetResetPolicy(getAutoOffsetReset(defaultReset, buildContext));
  }

  protected static String tableChangeLogOpName(final ExecutionStepPropertiesV1 props) {
    final List<String> parts = props.getQueryContext().getContext();
    Stacker stacker = new Stacker();
    for (final String part : parts.subList(0, parts.size() - 1)) {
      stacker = stacker.push(part);
    }
    return StreamsUtil.buildOpName(stacker.push("Reduce").getQueryContext());
  }

  protected static Function<Windowed<GenericKey>, Collection<?>> windowedKeyGenerator(
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

  protected static Topology.AutoOffsetReset getAutoOffsetReset(
      final Topology.AutoOffsetReset defaultValue,
      final RuntimeBuildContext buildContext) {
    final Object offestReset = buildContext.getKsqlConfig()
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

  protected static class AddKeyAndPseudoColumns<K>
      implements ValueTransformerWithKeySupplier<K, GenericRow, GenericRow> {

    private final Function<K, Collection<?>> keyGenerator;
    private final int pseudoColumnVersion;

    AddKeyAndPseudoColumns(
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

          final int numPseudoColumns = SystemColumns
              .pseudoColumnNames(pseudoColumnVersion).size();

          row.ensureAdditionalCapacity(numPseudoColumns + keyColumns.size());

          if (pseudoColumnVersion >= SystemColumns.ROWTIME_PSEUDOCOLUMN_VERSION) {
            final long timestamp = processorContext.timestamp();
            row.append(timestamp);
          }

          if (pseudoColumnVersion >= SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION) {
            final int partition = processorContext.partition();
            final long offset = processorContext.offset();
            row.append(partition);
            row.append(offset);
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
