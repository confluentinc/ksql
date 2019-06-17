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
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.physical.AddTimestampColumn;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.streams.MaterializedFactory;
import io.confluent.ksql.streams.StreamsUtil;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;

@Immutable
public class DataSourceNode
    extends PlanNode {

  private static final ValueMapperWithKey<String, GenericRow, GenericRow>
      nonWindowedValueMapper = (key, row) -> {
        if (row != null) {
          row.getColumns().add(0, key);
        }
        return row;
      };

  private static final ValueMapperWithKey<Windowed<String>, GenericRow, GenericRow>
      windowedMapper = (key, row) -> {
        if (row != null) {
          final Window window = key.window();
          final String end = window instanceof SessionWindow ? String.valueOf(window.end()) : "-";
          final String rowKey = String.format("%s : Window{start=%d end=%s}",
              key.key(), window.start(), end);
          row.getColumns().add(0, rowKey);
        }
        return row;
      };

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
    this.schema = dataSource.getSchema()
      .withAlias(alias);

    final Optional<String> keyFieldName = dataSource.getKeyField()
        .withAlias(alias)
        .name();

    this.keyField = KeyField.of(keyFieldName, dataSource.getKeyField().legacy())
        .validateKeyExistsIn(schema);

    this.materializedFactorySupplier =
        requireNonNull(materializedFactorySupplier, "materializedFactorySupplier");
  }

  public String getTopicName() {
    return dataSource.getKsqlTopicName();
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

  public String getAlias() {
    return alias;
  }

  @Override
  public int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    final String topicName = getDataSource().getKsqlTopic().getKafkaTopicName();

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

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId());
    final int timeStampColumnIndex = getTimeStampColumnIndex();
    final TimestampExtractor timestampExtractor = getTimestampExtractionPolicy()
        .create(timeStampColumnIndex);

    final KsqlSerdeFactory valueSerdeFactory = getDataSource()
        .getKsqlTopic()
        .getValueSerdeFactory();

    final Serde<GenericRow> streamSerde = builder.buildGenericRowSerde(
        valueSerdeFactory,
        PhysicalSchema.from(
            dataSource.getSchema().withoutImplicitAndKeyFieldsInValue(),
            dataSource.getSerdeOptions()
        ),
        contextStacker.push(SOURCE_OP_NAME).getQueryContext()
    );

    if (getDataSourceType() == DataSourceType.KTABLE) {
      final KsqlTable table = (KsqlTable) getDataSource();
      final QueryContext.Stacker reduceContextStacker = contextStacker.push(REDUCE_OP_NAME);

      final Serde<GenericRow> aggregateSerde = builder.buildGenericRowSerde(
          valueSerdeFactory,
          PhysicalSchema.from(schema, SerdeOption.none()),
          reduceContextStacker.getQueryContext()
      );

      final KTable<?, GenericRow> kTable = createKTable(
          builder.getStreamsBuilder(),
          getAutoOffsetReset(builder.getKsqlConfig().getKsqlStreamConfigProps()),
          streamSerde,
          aggregateSerde,
          timestampExtractor,
          builder.getKsqlConfig(),
          reduceContextStacker.getQueryContext()
      );
      return new SchemaKTable<>(
          schema,
          kTable,
          getKeyField(),
          new ArrayList<>(),
          table.getKeySerdeFactory(),
          SchemaKStream.Type.SOURCE,
          builder.getKsqlConfig(),
          builder.getFunctionRegistry(),
          contextStacker.getQueryContext()
      );
    }

    final KsqlStream stream = (KsqlStream) getDataSource();
    final KStream kstream = createKStream(
        builder.getStreamsBuilder(),
        timestampExtractor,
        streamSerde
    );

    return new SchemaKStream<>(
        getSchema(),
        kstream,
        getKeyField(),
        new ArrayList<>(),
        stream.getKeySerdeFactory(),
        SchemaKStream.Type.SOURCE,
        builder.getKsqlConfig(),
        builder.getFunctionRegistry(),
        contextStacker.getQueryContext()
    );
  }

  private static Topology.AutoOffsetReset getAutoOffsetReset(final Map<String, Object> props) {
    if (props.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
      final String offestReset = props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toString();
      if (offestReset.equalsIgnoreCase("EARLIEST")) {
        return Topology.AutoOffsetReset.EARLIEST;
      } else if (offestReset.equalsIgnoreCase("LATEST")) {
        return Topology.AutoOffsetReset.LATEST;
      }
    }
    return null;
  }

  private int getTimeStampColumnIndex() {
    final String timestampFieldName = getTimestampExtractionPolicy().timestampField();
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

  @SuppressWarnings("unchecked")
  private KStream<?, GenericRow> createKStream(
      final StreamsBuilder builder,
      final TimestampExtractor timestampExtractor,
      final Serde<GenericRow> streamSerde) {
    final KsqlStream ksqlStream = (KsqlStream) getDataSource();

    if (ksqlStream.hasWindowedKey()) {
      return stream(builder, timestampExtractor, streamSerde,
          (Serde<Windowed<String>>)ksqlStream.getKeySerdeFactory().create(), windowedMapper);
    }

    return stream(builder, timestampExtractor, streamSerde,
        (Serde<String>)ksqlStream.getKeySerdeFactory().create(), nonWindowedValueMapper);
  }

  private <K> KStream<K, GenericRow> stream(
      final StreamsBuilder builder,
      final TimestampExtractor timestampExtractor,
      final Serde<GenericRow> genericRowSerde,
      final Serde<K> keySerde,
      final ValueMapperWithKey<K, GenericRow, GenericRow> mapper) {

    final Consumed<K, GenericRow> consumed = Consumed
        .with(keySerde, genericRowSerde)
        .withTimestampExtractor(timestampExtractor);

    return builder
        .stream(getDataSource().getKsqlTopic().getKafkaTopicName(), consumed)
        .mapValues(mapper)
        .transformValues(new AddTimestampColumn());
  }

  @SuppressWarnings("unchecked")
  private KTable<?, GenericRow> createKTable(
      final StreamsBuilder builder, 
      final Topology.AutoOffsetReset autoOffsetReset,
      final Serde<GenericRow> streamSerde,
      final Serde<GenericRow> aggregateSerde,
      final TimestampExtractor timestampExtractor,
      final KsqlConfig ksqlConfig,
      final QueryContext reduceContextBuilder) {
    // to build a table we apply the following transformations:
    // 1. Create a KStream on the changelog topic.
    // 2. mapValues to add the ROWKEY column
    // 3. transformValues to add the ROWTIME column. transformValues is required to access the
    //    streams ProcessorContext which has the timestamp for the record. Also, transformValues
    //    is only available for KStream (not KTable). This is why we have to create a KStream
    //    first instead of a KTable.
    // 4. mapValues to transform null records into Optional<GenericRow>.EMPTY. We eventually need
    //    to aggregate the KStream to produce the KTable. However the KStream aggregator filters
    //    out records with null keys or values. For tables, a null value for a key represents
    //    that the key was deleted. So we preserve these "tombstone" records by converting them
    //    to a not-null representation.
    // 5. Aggregate the KStream into a KTable using a custom aggregator that handles Optional.EMPTY
    final KsqlTable ksqlTable = (KsqlTable) getDataSource();

    if (ksqlTable.isWindowed()) {
      return table(
          builder, autoOffsetReset, timestampExtractor, ksqlTable.getKsqlTopic(), windowedMapper,
          (Serde<Windowed<String>>)ksqlTable.getKeySerdeFactory().create(),
          streamSerde, aggregateSerde, ksqlConfig, reduceContextBuilder);
    }

    return table(
        builder, autoOffsetReset, timestampExtractor, ksqlTable.getKsqlTopic(),
        nonWindowedValueMapper, (Serde<String>)ksqlTable.getKeySerdeFactory().create(),
        streamSerde, aggregateSerde, ksqlConfig, reduceContextBuilder);
  }

  private <K> KTable<?, GenericRow> table(
      final StreamsBuilder builder,
      final Topology.AutoOffsetReset autoOffsetReset,
      final TimestampExtractor timestampExtractor,
      final KsqlTopic ksqlTopic,
      final ValueMapperWithKey<K, GenericRow, GenericRow> valueMapper,
      final Serde<K> keySerde,
      final Serde<GenericRow> streamSerde,
      final Serde<GenericRow> aggregateSerde,
      final KsqlConfig ksqlConfig,
      final QueryContext reduceContextBuilder
  ) {
    final Consumed<K, GenericRow> consumed = Consumed
        .with(keySerde, streamSerde)
        .withOffsetResetPolicy(autoOffsetReset)
        .withTimestampExtractor(timestampExtractor);

    final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        materializedFactorySupplier.apply(ksqlConfig).create(
            keySerde,
            aggregateSerde,
            StreamsUtil.buildOpName(reduceContextBuilder));
    return builder
        .stream(ksqlTopic.getKafkaTopicName(), consumed)
        .mapValues(valueMapper)
        .transformValues(new AddTimestampColumn())
        .mapValues(Optional::ofNullable)
        .groupByKey()
        .aggregate(
            () -> null,
            (k, value, oldValue) -> value.orElse(null),
            materialized);
  }

  public DataSourceType getDataSourceType() {
    return dataSource.getDataSourceType();
  }

  private TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return dataSource.getTimestampExtractionPolicy();
  }
}
