/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.physical.AddTimestampColumn;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;

@Immutable
public class StructuredDataSourceNode
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
          row.getColumns().add(0,
              String.format("%s : Window{start=%d end=-}", key
                  .key(), key.window().start())
          );
        }
        return row;
      };

  private final Serde<Windowed<String>> windowedSerde
          = WindowedSerdes.timeWindowedSerdeFrom(String.class);
  private final StructuredDataSource structuredDataSource;
  private final Schema schema;


  // TODO: pass in the "assignments" and the "outputs" separately
  // TODO: (i.e., get rid if the symbol := symbol idiom)
  @JsonCreator
  public StructuredDataSourceNode(
      @JsonProperty("id") final PlanNodeId id,
      @JsonProperty("structuredDataSource") final StructuredDataSource structuredDataSource,
      @JsonProperty("schema") Schema schema
  ) {
    super(id);
    Objects.requireNonNull(structuredDataSource, "structuredDataSource can't be null");
    Objects.requireNonNull(schema, "schema can't be null");
    this.schema = schema;
    this.structuredDataSource = structuredDataSource;
  }

  public String getTopicName() {
    return structuredDataSource.getTopicName();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public Field getKeyField() {
    return structuredDataSource.getKeyField();
  }

  public StructuredDataSource getStructuredDataSource() {
    return structuredDataSource;
  }

  @Override
  public int getPartitions(KafkaTopicClient kafkaTopicClient) {
    String topicName = getStructuredDataSource().getKsqlTopic().getKafkaTopicName();
    Map<String, TopicDescription> descriptions
        = kafkaTopicClient.describeTopics(Arrays.asList(topicName));
    if (!descriptions.containsKey(topicName)) {
      throw new KsqlException("Could not get topic description for " + topicName);
    }
    return descriptions.get(topicName).partitions().size();
  }

  @Override
  public List<PlanNode> getSources() {
    return null;
  }

  @Override
  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitStructuredDataSourceNode(this, context);
  }

  @Override
  public SchemaKStream buildStream(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> props,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    if (!(getTimestampExtractionPolicy() instanceof MetadataTimestampExtractionPolicy)) {
      ksqlConfig.put(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX,
          getTimeStampColumnIndex());
    }
    KsqlTopicSerDe ksqlTopicSerDe = getStructuredDataSource()
        .getKsqlTopic().getKsqlTopicSerDe();
    Serde<GenericRow> genericRowSerde =
        ksqlTopicSerDe.getGenericRowSerde(
            SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(
                getSchema()), ksqlConfig, false, schemaRegistryClient);

    if (getDataSourceType() == StructuredDataSource.DataSourceType.KTABLE) {
      final KsqlTable table = (KsqlTable) getStructuredDataSource();

      final KTable kTable = createKTable(
          builder,
          getAutoOffsetReset(props),
          table,
          genericRowSerde,
          table.getKsqlTopic().getKsqlTopicSerDe().getGenericRowSerde(
              getSchema(), ksqlConfig, true, schemaRegistryClient)
      );
      return new SchemaKTable(
          getSchema(),
          kTable,
          getKeyField(),
          new ArrayList<>(),
          table.isWindowed(),
          SchemaKStream.Type.SOURCE,
          functionRegistry,
          schemaRegistryClient
      );
    }

    return new SchemaKStream(
        getSchema(),
        builder.stream(
            getStructuredDataSource().getKsqlTopic().getKafkaTopicName(),
            Consumed.with(Serdes.String(), genericRowSerde)
        ).mapValues(nonWindowedValueMapper).transformValues(new AddTimestampColumn()),
        getKeyField(), new ArrayList<>(),
        SchemaKStream.Type.SOURCE, functionRegistry, schemaRegistryClient
    );
  }

  private Topology.AutoOffsetReset getAutoOffsetReset(Map<String, Object> props) {
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
    String timestampFieldName = getTimestampExtractionPolicy().timestampField();
    if (timestampFieldName.contains(".")) {
      for (int i = 2; i < schema.fields().size(); i++) {
        Field field = schema.fields().get(i);
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
    } else {
      for (int i = 2; i < schema.fields().size(); i++) {
        Field field = schema.fields().get(i);
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

  private KTable createKTable(
      StreamsBuilder builder, final Topology.AutoOffsetReset autoOffsetReset,
      final KsqlTable ksqlTable,
      final Serde<GenericRow> genericRowSerde,
      final Serde<GenericRow> genericRowSerdeAfterRead
  ) {
    if (ksqlTable.isWindowed()) {
      return table(
          builder.stream(
              ksqlTable.getKsqlTopic().getKafkaTopicName(),
              Consumed.with(windowedSerde, genericRowSerde).withOffsetResetPolicy(autoOffsetReset)
          ).mapValues(windowedMapper).transformValues(new AddTimestampColumn()),
          windowedSerde,
          genericRowSerdeAfterRead
      );
    } else {
      return table(
          builder.stream(
              ksqlTable.getKsqlTopic().getKafkaTopicName(),
              Consumed.with(Serdes.String(), genericRowSerde).withOffsetResetPolicy(autoOffsetReset)
          ).mapValues(nonWindowedValueMapper).transformValues(new AddTimestampColumn()),
          Serdes.String(),
          genericRowSerdeAfterRead
      );
    }
  }

  private <K> KTable table(
      final KStream<K, GenericRow> stream,
      final Serde<K> keySerde,
      final Serde<GenericRow> valueSerde
  ) {
    return stream.groupByKey(Serialized.with(keySerde, valueSerde))
        .reduce((genericRow, newValue) -> newValue);
  }

  public StructuredDataSource.DataSourceType getDataSourceType() {
    return structuredDataSource.getDataSourceType();
  }

  public TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return structuredDataSource.getTimestampExtractionPolicy();
  }
}
