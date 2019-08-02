/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.planner.plan;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.physical.AddTimestampColumn;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.QueryContext.Stacker;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KsqlConsumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;

@Immutable
public class DataSourceNode extends PlanNode {

  private static final String SOURCE_OP_NAME = "source";
  private static final String REDUCE_OP_NAME = "reduce";

  private final DataSource<?> dataSource;
  private final String alias;
  private final LogicalSchema schema;
  private final KeyField keyField;
  private final Function<KsqlConfig, MaterializedFactory> materializedFactorySupplier;

  public DataSourceNode(
      final PlanNodeId id,
      final DataSource<?> dataSource,
      final String alias
  ) {
    this(id, dataSource, alias, MaterializedFactory::create);
  }

  @VisibleForTesting
  DataSourceNode(
      final PlanNodeId id,
      final DataSource<?> dataSource,
      final String alias,
      final Function<KsqlConfig, MaterializedFactory> materializedFactorySupplier
  ) {
    super(id, dataSource.getDataSourceType());
    this.dataSource = requireNonNull(dataSource, "dataSource");
    this.alias = requireNonNull(alias, "alias");

    // DataSourceNode copies implicit and key fields into the value schema
    // It users a KS valueMapper to add the key fields
    // and a KS transformValues to add the implicit fields
    this.schema = dataSource.getSchema()
        .withMetaAndKeyFieldsInValue()
        .withAlias(alias);

    final Optional<String> keyFieldName = dataSource.getKeyField()
        .withAlias(alias)
        .name();

    this.keyField = KeyField.of(keyFieldName, dataSource.getKeyField().legacy())
        .validateKeyExistsIn(schema);

    this.materializedFactorySupplier =
        requireNonNull(materializedFactorySupplier, "materializedFactorySupplier");
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  public DataSource<?> getDataSource() {
    return dataSource;
  }

  String getAlias() {
    return alias;
  }

  public DataSourceType getDataSourceType() {
    return dataSource.getDataSourceType();
  }

  @Override
  public int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    final String topicName = dataSource.getKsqlTopic().getKafkaTopicName();

    return kafkaTopicClient.describeTopic(topicName)
        .partitions()
        .size();
  }

  @Override
  public List<PlanNode> getSources() {
    return null;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitDataSourceNode(this, context);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    final Optional<WindowInfo> windowInfo = dataSource
        .getKsqlTopic()
        .getKeyFormat()
        .getWindowInfo();

    if (windowInfo.isPresent()) {
      final KeySerdeSupplier<Windowed<Struct>> keySerdeSupplier =
          (format, physicalSchema, logContext) ->
              builder.buildKeySerde(format, windowInfo.get(), physicalSchema, logContext);

      return buildStream(builder, keySerdeSupplier, windowedMapper());
    }

    return buildStream(builder, builder::buildKeySerde, nonWindowedValueMapper());
  }

  private <K> SchemaKStream<K> buildStream(
      final KsqlQueryBuilder builder,
      final KeySerdeSupplier<K> keySerdeSupplier,
      final ValueMapperWithKey<K, GenericRow, GenericRow> mapper
  ) {
    final Stacker contextStacker = builder.buildNodeContext(getId());

    final Consumed<K, GenericRow> consumed =
        buildSourceConsumed(builder, keySerdeSupplier, contextStacker);

    return getDataSourceType() == DataSourceType.KTABLE
        ? buildSourceTable(builder, consumed, mapper, contextStacker)
        : buildSourceStream(builder, consumed, mapper, contextStacker);
  }

  private <K> SchemaKTable<K> buildSourceTable(
      final KsqlQueryBuilder builder,
      final Consumed<K, GenericRow> consumed,
      final ValueMapperWithKey<K, GenericRow, GenericRow> mapper,
      final Stacker contextStacker
  ) {
    final KTable<K, GenericRow> kTable = createKTable(builder, consumed, mapper, contextStacker);

    final KeySerde<K> keySerde = (KeySerde<K>) KsqlConsumed.keySerde(consumed);

    return new SchemaKTable<>(
        kTable,
        schema,
        keySerde,
        keyField,
        ImmutableList.of(),
        SchemaKStream.Type.SOURCE,
        builder.getKsqlConfig(),
        builder.getFunctionRegistry(),
        contextStacker.getQueryContext()
    );
  }

  private <K> SchemaKStream<K> buildSourceStream(
      final KsqlQueryBuilder builder,
      final Consumed<K, GenericRow> consumed,
      final ValueMapperWithKey<K, GenericRow, GenericRow> mapper,
      final Stacker contextStacker
  ) {
    final KStream<K, GenericRow> kstream = createKStream(builder, consumed, mapper);

    final KeySerde<K> keySerde = (KeySerde<K>) KsqlConsumed.keySerde(consumed);

    return new SchemaKStream<>(
        kstream,
        schema,
        keySerde,
        keyField,
        ImmutableList.of(),
        SchemaKStream.Type.SOURCE,
        builder.getKsqlConfig(),
        builder.getFunctionRegistry(),
        contextStacker.getQueryContext()
    );
  }


  private <K> KTable<K, GenericRow> createKTable(
      final KsqlQueryBuilder builder,
      final Consumed<K, GenericRow> consumed,
      final ValueMapperWithKey<K, GenericRow, GenericRow> mapper,
      final Stacker contextStacker
  ) {
    final Topology.AutoOffsetReset autoOffsetReset =
        getAutoOffsetReset(builder.getKsqlConfig().getKsqlStreamConfigProps());

    final Consumed<K, GenericRow> tableConsumed = consumed
        .withOffsetResetPolicy(autoOffsetReset);

    final Stacker reduceContextStacker = contextStacker.push(REDUCE_OP_NAME);

    final Serde<GenericRow> aggregateSerde = builder.buildValueSerde(
        dataSource.getKsqlTopic().getValueFormat().getFormatInfo(),
        PhysicalSchema.from(schema, SerdeOption.none()),
        reduceContextStacker.getQueryContext()
    );

    final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        materializedFactorySupplier.apply(builder.getKsqlConfig()).create(
            KsqlConsumed.keySerde(tableConsumed),
            aggregateSerde,
            StreamsUtil.buildOpName(reduceContextStacker.getQueryContext()));

    // to build a table we apply the following transformations:
    return builder.getStreamsBuilder()

        // 1. Create a KStream on the changelog topic.
        .stream(dataSource.getKsqlTopic().getKafkaTopicName(), tableConsumed)

        // 2. mapValues to add the ROWKEY column
        .mapValues(mapper)

        // 3. transformValues to add the ROWTIME column. transformValues is required to access the
        //    streams ProcessorContext which has the timestamp for the record. Also, transformValues
        //    is only available for KStream (not KTable). This is why we have to create a KStream
        //    first instead of a KTable.
        .transformValues(new AddTimestampColumn())

        // 4. mapValues to transform null records into Optional<GenericRow>.EMPTY. We eventually
        //    need to aggregate the KStream to produce the KTable. However the KStream aggregator
        //    filters out records with null keys or values. For tables, a null value for a key
        //    represents that the key was deleted. So we preserve these "tombstone" records by
        //    converting them to a not-null representation.
        .mapValues(Optional::ofNullable)

        // 5. Group by the key, so that we can:
        .groupByKey()

        // 6. Aggregate the KStream into a KTable using a custom aggregator that handles
        // Optional.EMPTY
        .aggregate(
            () -> null,
            (k, value, oldValue) -> value.orElse(null),
            materialized);
  }

  private <K> KStream<K, GenericRow> createKStream(
      final KsqlQueryBuilder builder,
      final Consumed<K, GenericRow> consumed,
      final ValueMapperWithKey<K, GenericRow, GenericRow> mapper
  ) {
    // to build a stream we apply the following transformations:
    return builder.getStreamsBuilder()

        // 1. Create a KStream on the changelog topic.
        .stream(dataSource.getKsqlTopic().getKafkaTopicName(), consumed)

        // 2. mapValues to add the ROWKEY column
        .mapValues(mapper)

        // 3. transformValues to add the ROWTIME column. transformValues is required to access the
        //    streams ProcessorContext which has the timestamp for the record.
        .transformValues(new AddTimestampColumn());
  }

  private <K> Consumed<K, GenericRow> buildSourceConsumed(
      final KsqlQueryBuilder builder,
      final KeySerdeSupplier<K> keySerdeSupplier,
      final Stacker contextStacker
  ) {
    final QueryContext logContext = contextStacker
        .push(SOURCE_OP_NAME)
        .getQueryContext();

    final PhysicalSchema schema = PhysicalSchema.from(
        dataSource.getSchema(),
        dataSource.getSerdeOptions()
    );

    final KsqlTopic topic = dataSource.getKsqlTopic();
    final KeyFormat keyFormat = topic.getKeyFormat();
    final FormatInfo format = keyFormat.getFormatInfo();

    final Serde<K> keySerde = keySerdeSupplier.buildKeySerde(format, schema, logContext);

    final Serde<GenericRow> valueSerde = builder
        .buildValueSerde(topic.getValueFormat().getFormatInfo(), schema, logContext);

    final TimestampExtractor timestampExtractor = dataSource.getTimestampExtractionPolicy()
        .create(getTimeStampColumnIndex());

    return Consumed
        .with(keySerde, valueSerde)
        .withTimestampExtractor(timestampExtractor);
  }

  private static Topology.AutoOffsetReset getAutoOffsetReset(final Map<String, Object> props) {
    final Object offestReset = props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    if (offestReset == null) {
      return null;
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

  private int getTimeStampColumnIndex() {
    final String timestampFieldName = dataSource.getTimestampExtractionPolicy().timestampField();
    if (timestampFieldName == null) {
      return -1;
    }
    if (timestampFieldName.contains(".")) {
      final Integer index = findMatchingTimestampField(timestampFieldName);
      if (index != null) {
        return index;
      }
    } else {
      for (int i = 2; i < schema.valueFields().size(); i++) {
        final Field field = schema.valueFields().get(i);
        if (field.name().contains(".")) {
          if (timestampFieldName.equals(field.name().substring(field.name().indexOf(".") + 1))) {
            return i - 2;
          }
        } else {
          if (timestampFieldName.equals(field.name())) {
            return i - 2;
          }
        }
      }
    }
    return -1;
  }

  private Integer findMatchingTimestampField(final String timestampFieldName) {
    for (int i = 2; i < schema.valueFields().size(); i++) {
      final Field field = schema.valueFields().get(i);
      if (field.name().contains(".")) {
        if (timestampFieldName.equals(field.name())) {
          return i - 2;
        }
      } else {
        if (timestampFieldName
            .substring(timestampFieldName.indexOf(".") + 1)
            .equals(field.name())) {
          return i - 2;
        }
      }
    }
    return null;
  }

  private org.apache.kafka.connect.data.Field getKeySchemaSingleField() {
    if (schema.keySchema().fields().size() != 1) {
      throw new IllegalStateException("Only single key fields are currently supported");
    }

    return schema.keySchema().fields().get(0);
  }

  private ValueMapperWithKey<Windowed<Struct>, GenericRow, GenericRow> windowedMapper() {
    final org.apache.kafka.connect.data.Field keyField = getKeySchemaSingleField();
    return (keyStruct, row) -> {
      if (row != null) {
        final Window window = keyStruct.window();
        final long start = window.start();
        final String end = window instanceof SessionWindow ? String.valueOf(window.end()) : "-";
        final Object key = keyStruct.key().get(keyField);
        final String rowKey = String.format("%s : Window{start=%d end=%s}", key, start, end);
        row.getColumns().add(0, rowKey);
      }
      return row;
    };
  }

  private ValueMapperWithKey<Struct, GenericRow, GenericRow> nonWindowedValueMapper() {
    final org.apache.kafka.connect.data.Field keyField = getKeySchemaSingleField();
    return (key, row) -> {
      if (row != null) {
        row.getColumns().add(0, key.get(keyField));
      }
      return row;
    };
  }

  private interface KeySerdeSupplier<K> {

    KeySerde<K> buildKeySerde(
        FormatInfo format,
        PhysicalSchema schema,
        QueryContext queryContext
    );
  }
}
