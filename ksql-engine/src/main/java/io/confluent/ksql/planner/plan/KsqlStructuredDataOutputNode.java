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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

public class KsqlStructuredDataOutputNode extends OutputNode {

  private final String kafkaTopicName;
  private final KsqlTopic ksqlTopic;
  private final Field keyField;
  private final Field timestampField;
  private final Map<String, Object> outputProperties;


  @JsonCreator
  public KsqlStructuredDataOutputNode(
      @JsonProperty("id") final PlanNodeId id,
      @JsonProperty("source") final PlanNode source,
      @JsonProperty("schema") final Schema schema,
      @JsonProperty("timestamp") final Field timestampField,
      @JsonProperty("key") final Field keyField,
      @JsonProperty("ksqlTopic") final KsqlTopic ksqlTopic,
      @JsonProperty("topicName") final String topicName,
      @JsonProperty("outputProperties") final Map<String, Object> outputProperties,
      @JsonProperty("limit") final Optional<Integer> limit
  ) {
    super(id, source, schema, limit);
    this.kafkaTopicName = topicName;
    this.keyField = keyField;
    this.timestampField = timestampField;
    this.ksqlTopic = ksqlTopic;
    this.outputProperties = outputProperties;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  @Override
  public Field getKeyField() {
    return keyField;
  }

  @Override
  public SchemaKStream buildStream(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient,
      final MetastoreUtil metastoreUtil,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> props,
      final SchemaRegistryClient schemaRegistryClient
  ) {

    final SchemaKStream schemaKStream = getSource().buildStream(
        builder,
        ksqlConfig,
        kafkaTopicClient,
        metastoreUtil,
        functionRegistry,
        props,
        schemaRegistryClient
    );
    final Set<Integer> rowkeyIndexes = SchemaUtil.getRowTimeRowKeyIndexes(getSchema());
    final Builder outputNodeBuilder = Builder.from(this);
    final Schema schema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(getSchema());
    outputNodeBuilder.withSchema(schema);

    if (getTopicSerde() instanceof KsqlAvroTopicSerDe) {
      addAvroSchemaToResultTopic(outputNodeBuilder);
    }

    final Map<String, Object> outputProperties = getOutputProperties();

    if (outputProperties.containsKey(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY)) {
      ksqlConfig.put(
          KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY,
          outputProperties.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY)
      );
    }
    if (outputProperties.containsKey(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY)) {
      ksqlConfig.put(
          KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY,
          outputProperties.get(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY)
      );
    }

    final SchemaKStream result = createOutputStream(
        schemaKStream,
        outputNodeBuilder,
        functionRegistry,
        outputProperties,
        schemaRegistryClient
    );

    final KsqlStructuredDataOutputNode noRowKey = outputNodeBuilder.build();
    createSinkTopic(noRowKey.getKafkaTopicName(), ksqlConfig, kafkaTopicClient);
    result.into(
        noRowKey.getKafkaTopicName(),
        noRowKey.getKsqlTopic().getKsqlTopicSerDe()
            .getGenericRowSerde(noRowKey.getSchema(), ksqlConfig, false, schemaRegistryClient),
        rowkeyIndexes
    );

    result.setOutputNode(
        outputNodeBuilder
            .withSchema(
                SchemaUtil.addImplicitRowTimeRowKeyToSchema(noRowKey.getSchema()))
            .build());
    return result;
  }

  private SchemaKStream createOutputStream(
      final SchemaKStream schemaKStream,
      final KsqlStructuredDataOutputNode.Builder outputNodeBuilder,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> outputProperties,
      final SchemaRegistryClient schemaRegistryClient
  ) {

    if (schemaKStream instanceof SchemaKTable) {
      return schemaKStream;
    }

    final SchemaKStream result = new SchemaKStream(
        getSchema(),
        schemaKStream.getKstream(),
        this.getKeyField(),
        Collections.singletonList(schemaKStream),
        SchemaKStream.Type.SINK,
        functionRegistry,
        schemaRegistryClient
    );

    if (outputProperties.containsKey(DdlConfig.PARTITION_BY_PROPERTY)) {
      String keyFieldName = outputProperties.get(DdlConfig.PARTITION_BY_PROPERTY).toString();
      Field keyField = SchemaUtil.getFieldByName(
          result.getSchema(), keyFieldName)
          .orElseThrow(() -> new KsqlException(String.format(
              "Column %s does not exist in the result schema."
              + " Error in Partition By clause.",
              keyFieldName
          )));

      outputNodeBuilder.withKeyField(keyField);
      return result.selectKey(keyField);
    }
    return result;
  }

  private void addAvroSchemaToResultTopic(final Builder builder) {
    final KsqlAvroTopicSerDe ksqlAvroTopicSerDe =
        new KsqlAvroTopicSerDe();
    builder.withKsqlTopic(new KsqlTopic(
        getKsqlTopic().getName(),
        getKsqlTopic().getKafkaTopicName(),
        ksqlAvroTopicSerDe
    ));
  }

  private void createSinkTopic(
      final String kafkaTopicName,
      KsqlConfig ksqlConfig,
      KafkaTopicClient kafkaTopicClient
  ) {
    int numberOfPartitions =
        (Integer) ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY);
    short numberOfReplications =
        (Short) ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY);
    kafkaTopicClient.createTopic(kafkaTopicName, numberOfPartitions, numberOfReplications);
  }

  public Field getTimestampField() {
    return timestampField;
  }

  public KsqlTopic getKsqlTopic() {
    return ksqlTopic;
  }

  public Map<String, Object> getOutputProperties() {
    return outputProperties;
  }

  public KsqlTopicSerDe getTopicSerde() {
    return ksqlTopic.getKsqlTopicSerDe();
  }

  public static class Builder {

    private PlanNodeId id;
    private PlanNode source;
    private Schema schema;
    private Field timestampField;
    private Field keyField;
    private KsqlTopic ksqlTopic;
    private String topicName;
    private Map<String, Object> outputProperties;
    private Optional<Integer> limit;

    public KsqlStructuredDataOutputNode build() {
      return new KsqlStructuredDataOutputNode(
          id,
          source,
          schema,
          timestampField,
          keyField,
          ksqlTopic,
          topicName,
          outputProperties,
          limit
      );
    }

    public static Builder from(final KsqlStructuredDataOutputNode original) {
      return new Builder()
          .withId(original.getId())
          .withSource(original.getSource())
          .withSchema(original.getSchema())
          .withTimestampField(original.getTimestampField())
          .withKeyField(original.getKeyField())
          .withKsqlTopic(original.getKsqlTopic())
          .withTopicName(original.getKafkaTopicName())
          .withOutputProperties(original.getOutputProperties())
          .withLimit(original.getLimit());
    }

    Builder withLimit(final Optional<Integer> limit) {
      this.limit = limit;
      return this;
    }

    Builder withOutputProperties(Map<String, Object> outputProperties) {
      this.outputProperties = outputProperties;
      return this;
    }

    Builder withTopicName(String topicName) {
      this.topicName = topicName;
      return this;
    }

    Builder withKsqlTopic(KsqlTopic ksqlTopic) {
      this.ksqlTopic = ksqlTopic;
      return this;
    }

    Builder withKeyField(Field keyField) {
      this.keyField = keyField;
      return this;
    }

    Builder withTimestampField(Field timestampField) {
      this.timestampField = timestampField;
      return this;
    }

    Builder withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    Builder withSource(PlanNode source) {
      this.source = source;
      return this;
    }

    Builder withId(final PlanNodeId id) {
      this.id = id;
      return this;
    }
  }
}
