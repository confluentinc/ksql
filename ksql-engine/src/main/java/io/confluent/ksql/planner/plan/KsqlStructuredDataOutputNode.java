/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;

public class KsqlStructuredDataOutputNode extends OutputNode {

  private final String kafkaTopicName;
  private final KsqlTopic ksqlTopic;
  private final Field keyField;
  private final boolean doCreateInto;
  private final Map<String, Object> outputProperties;


  @JsonCreator
  public KsqlStructuredDataOutputNode(
      @JsonProperty("id") final PlanNodeId id,
      @JsonProperty("source") final PlanNode source,
      @JsonProperty("schema") final Schema schema,
      @JsonProperty("timestamp") final TimestampExtractionPolicy timestampExtractionPolicy,
      @JsonProperty("key") final Field keyField,
      @JsonProperty("ksqlTopic") final KsqlTopic ksqlTopic,
      @JsonProperty("topicName") final String kafkaTopicName,
      @JsonProperty("outputProperties") final Map<String, Object> outputProperties,
      @JsonProperty("limit") final Optional<Integer> limit,
      @JsonProperty("doCreateInto") final boolean doCreateInto) {
    super(id, source, schema, limit, timestampExtractionPolicy);
    this.kafkaTopicName = kafkaTopicName;
    this.keyField = keyField;
    this.ksqlTopic = ksqlTopic;
    this.outputProperties = outputProperties;
    this.doCreateInto = doCreateInto;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  public boolean isDoCreateInto() {
    return doCreateInto;
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
      final FunctionRegistry functionRegistry,
      final Map<String, Object> props,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final PlanNode source = getSource();
    final SchemaKStream schemaKStream = source.buildStream(
        builder,
        ksqlConfig,
        kafkaTopicClient,
        functionRegistry,
        props,
        schemaRegistryClientFactory
    );

    final Set<Integer> rowkeyIndexes = SchemaUtil.getRowTimeRowKeyIndexes(getSchema());
    final Builder outputNodeBuilder = Builder.from(this);
    final Schema schema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(getSchema());
    outputNodeBuilder.withSchema(schema);

    if (getTopicSerde() instanceof KsqlAvroTopicSerDe) {
      addAvroSchemaToResultTopic(outputNodeBuilder);
    }

    final int partitions = (Integer) outputProperties.getOrDefault(
        KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY,
        ksqlConfig.getInt(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY));
    final short replicas = (Short) outputProperties.getOrDefault(
        KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY,
        ksqlConfig.getShort(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY));

    final SchemaKStream result = createOutputStream(
        schemaKStream,
        outputNodeBuilder,
        ksqlConfig,
        functionRegistry,
        outputProperties,
        schemaRegistryClientFactory
    );

    final KsqlStructuredDataOutputNode noRowKey = outputNodeBuilder.build();
    if (doCreateInto) {
      createSinkTopic(
          noRowKey.getKafkaTopicName(),
          kafkaTopicClient,
          shouldBeCompacted(result),
          partitions,
          replicas);
    }
    result.into(
        noRowKey.getKafkaTopicName(),
        noRowKey.getKsqlTopic().getKsqlTopicSerDe()
            .getGenericRowSerde(
                noRowKey.getSchema(), ksqlConfig, false, schemaRegistryClientFactory),
        rowkeyIndexes
    );

    result.setOutputNode(
        outputNodeBuilder
            .withSchema(
                SchemaUtil.addImplicitRowTimeRowKeyToSchema(noRowKey.getSchema()))
            .build());
    return result;
  }

  private boolean shouldBeCompacted(final SchemaKStream result) {
    return (result instanceof SchemaKTable)
           && !((SchemaKTable) result).isWindowed();
  }

  @SuppressWarnings("unchecked")
  private SchemaKStream createOutputStream(
      final SchemaKStream schemaKStream,
      final KsqlStructuredDataOutputNode.Builder outputNodeBuilder,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> outputProperties,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
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
        ksqlConfig,
        functionRegistry,
        schemaRegistryClientFactory.get()
    );

    if (outputProperties.containsKey(DdlConfig.PARTITION_BY_PROPERTY)) {
      final String keyFieldName = outputProperties.get(DdlConfig.PARTITION_BY_PROPERTY).toString();
      final Field keyField = SchemaUtil.getFieldByName(
          result.getSchema(), keyFieldName)
          .orElseThrow(() -> new KsqlException(String.format(
              "Column %s does not exist in the result schema."
              + " Error in Partition By clause.",
              keyFieldName
          )));

      outputNodeBuilder.withKeyField(keyField);
      return result.selectKey(keyField, false);
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
      final KafkaTopicClient kafkaTopicClient,
      final boolean isCompacted,
      final int numberOfPartitions,
      final short numberOfReplications
  ) {
    final Map<String, ?> config = isCompacted
        ? ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        : Collections.emptyMap();

    kafkaTopicClient.createTopic(kafkaTopicName,
                                 numberOfPartitions,
                                 numberOfReplications,
                                 config
    );
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

  public KsqlStructuredDataOutputNode cloneWithDoCreateInto(final boolean newDoCreateInto) {
    return new KsqlStructuredDataOutputNode(
        getId(),
        getSource(),
        getSchema(),
        getTimestampExtractionPolicy(),
        getKeyField(),
        getKsqlTopic(),
        getKafkaTopicName(),
        getOutputProperties(),
        getLimit(),
        newDoCreateInto
    );
  }

  public static class Builder {

    private PlanNodeId id;
    private PlanNode source;
    private Schema schema;
    private TimestampExtractionPolicy timestampExtractionPolicy;
    private Field keyField;
    private KsqlTopic ksqlTopic;
    private String kafkaTopicName;
    private Map<String, Object> outputProperties;
    private Optional<Integer> limit;
    private boolean doCreateInto;

    public KsqlStructuredDataOutputNode build() {
      return new KsqlStructuredDataOutputNode(
          id,
          source,
          schema,
          timestampExtractionPolicy,
          keyField,
          ksqlTopic,
          kafkaTopicName,
          outputProperties,
          limit,
          doCreateInto);
    }

    public static Builder from(final KsqlStructuredDataOutputNode original) {
      return new Builder()
          .withId(original.getId())
          .withSource(original.getSource())
          .withSchema(original.getSchema())
          .withTimestampExtractionPolicy(original.getTimestampExtractionPolicy())
          .withKeyField(original.getKeyField())
          .withKsqlTopic(original.getKsqlTopic())
          .withKafkaTopicName(original.getKafkaTopicName())
          .withOutputProperties(original.getOutputProperties())
          .withLimit(original.getLimit())
          .withDoCreateInto(original.isDoCreateInto());
    }


    Builder withLimit(final Optional<Integer> limit) {
      this.limit = limit;
      return this;
    }

    Builder withOutputProperties(final Map<String, Object> outputProperties) {
      this.outputProperties = outputProperties;
      return this;
    }

    Builder withKafkaTopicName(final String kafkaTopicName) {
      this.kafkaTopicName = kafkaTopicName;
      return this;
    }

    Builder withKsqlTopic(final KsqlTopic ksqlTopic) {
      this.ksqlTopic = ksqlTopic;
      return this;
    }

    Builder withKeyField(final Field keyField) {
      this.keyField = keyField;
      return this;
    }

    Builder withTimestampExtractionPolicy(final TimestampExtractionPolicy extractionPolicy) {
      this.timestampExtractionPolicy = extractionPolicy;
      return this;
    }

    Builder withSchema(final Schema schema) {
      this.schema = schema;
      return this;
    }

    Builder withSource(final PlanNode source) {
      this.source = source;
      return this;
    }

    Builder withId(final PlanNodeId id) {
      this.id = id;
      return this;
    }

    Builder withDoCreateInto(final boolean doCreateInto) {
      this.doCreateInto = doCreateInto;
      return this;
    }

  }
}
