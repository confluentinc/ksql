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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KsqlStructuredDataOutputNode extends OutputNode {
  private final String kafkaTopicName;
  private final KsqlTopic ksqlTopic;
  private final Optional<Field> keyField;
  private final boolean doCreateInto;
  private final Map<String, Object> outputProperties;

  @JsonCreator
  public KsqlStructuredDataOutputNode(
      @JsonProperty("id") final PlanNodeId id,
      @JsonProperty("source") final PlanNode source,
      @JsonProperty("schema") final Schema schema,
      @JsonProperty("timestamp") final TimestampExtractionPolicy timestampExtractionPolicy,
      @JsonProperty("key") final Optional<Field> keyField,
      @JsonProperty("ksqlTopic") final KsqlTopic ksqlTopic,
      @JsonProperty("topicName") final String kafkaTopicName,
      @JsonProperty("outputProperties") final Map<String, Object> outputProperties,
      @JsonProperty("limit") final Optional<Integer> limit,
      @JsonProperty("doCreateInto") final boolean doCreateInto) {
    super(id, source, schema, limit, timestampExtractionPolicy);
    this.kafkaTopicName = kafkaTopicName;
    this.keyField = Objects.requireNonNull(keyField, "keyField");
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
  public QueryId getQueryId(final QueryIdGenerator queryIdGenerator) {
    final String base = queryIdGenerator.getNextId();
    if (!doCreateInto) {
      return new QueryId("InsertQuery_" + base);
    }
    if (getNodeOutputType().equals(DataSourceType.KTABLE)) {
      return new QueryId("CTAS_" + getId().toString() + "_" + base);
    }
    return new QueryId("CSAS_" + getId().toString() + "_" + base);
  }

  @Override
  public Optional<Field> getKeyField() {
    return keyField;
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    final PlanNode source = getSource();
    final SchemaKStream schemaKStream = source.buildStream(builder);

    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId());

    final Set<Integer> rowkeyIndexes = SchemaUtil.getRowTimeRowKeyIndexes(getSchema());
    final Builder outputNodeBuilder = Builder.from(this);
    final Schema schema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(getSchema());
    outputNodeBuilder.withSchema(schema);

    final int partitions = (Integer) outputProperties.getOrDefault(
        KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY,
        ksqlConfig.getInt(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY));
    final short replicas = (Short) outputProperties.getOrDefault(
        KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY,
        ksqlConfig.getShort(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY));

    final SchemaKStream<?> result = createOutputStream(
        schemaKStream,
        outputNodeBuilder,
        builder.getKsqlConfig(),
        builder.getFunctionRegistry(),
        outputProperties,
        contextStacker
    );

    final KsqlStructuredDataOutputNode noRowKey = outputNodeBuilder.build();
    final KsqlTopicSerDe ksqlTopicSerDe = noRowKey
        .getKsqlTopic()
        .getKsqlTopicSerDe();

    final Serde<GenericRow> outputRowSerde = builder.buildGenericRowSerde(
        ksqlTopicSerDe,
        noRowKey.getSchema(),
        contextStacker.getQueryContext());

    result.into(
        noRowKey.getKafkaTopicName(),
        outputRowSerde,
        rowkeyIndexes
    );

    result.setOutputNode(
        outputNodeBuilder
            .withSchema(
                SchemaUtil.addImplicitRowTimeRowKeyToSchema(noRowKey.getSchema()))
            .build());
    return result;
  }

  @SuppressWarnings("unchecked")
  private SchemaKStream<?> createOutputStream(
      final SchemaKStream schemaKStream,
      final KsqlStructuredDataOutputNode.Builder outputNodeBuilder,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> outputProperties,
      final QueryContext.Stacker contextStacker
  ) {

    if (schemaKStream instanceof SchemaKTable) {
      return schemaKStream;
    }

    final SchemaKStream result = new SchemaKStream(
        getSchema(),
        schemaKStream.getKstream(),
        this.getKeyField(),
        Collections.singletonList(schemaKStream),
        schemaKStream.getKeySerdeFactory(),
        SchemaKStream.Type.SINK,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
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

      outputNodeBuilder.withKeyField(Optional.of(keyField));
      return result.selectKey(keyField, false, contextStacker);
    }
    return result;
  }

  private static void createSinkTopic(
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

  private KsqlTopicSerDe getTopicSerde() {
    return ksqlTopic.getKsqlTopicSerDe();
  }

  public static class Builder {

    private PlanNodeId id;
    private PlanNode source;
    private Schema schema;
    private TimestampExtractionPolicy timestampExtractionPolicy;
    private Optional<Field> keyField;
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
          .withOutputProperties(original.outputProperties)
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

    Builder withKeyField(final Optional<Field> keyField) {
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