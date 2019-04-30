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

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.KsqlTopicSerDe;
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
  private final KeyField keyField;
  private final boolean doCreateInto;
  private final Map<String, Object> outputProperties;

  @JsonCreator
  public KsqlStructuredDataOutputNode(
      @JsonProperty("id") final PlanNodeId id,
      @JsonProperty("source") final PlanNode source,
      @JsonProperty("schema") final Schema schema,
      @JsonProperty("timestamp") final TimestampExtractionPolicy timestampExtractionPolicy,
      @JsonProperty("key") final KeyField keyField,
      @JsonProperty("ksqlTopic") final KsqlTopic ksqlTopic,
      @JsonProperty("topicName") final String kafkaTopicName,
      @JsonProperty("outputProperties") final Map<String, Object> outputProperties,
      @JsonProperty("limit") final Optional<Integer> limit,
      @JsonProperty("doCreateInto") final boolean doCreateInto
  ) {
    super(id, source, schema, limit, timestampExtractionPolicy);
    this.kafkaTopicName = Objects.requireNonNull(kafkaTopicName, "kafkaTopicName");
    this.keyField = Objects.requireNonNull(keyField, "keyField")
        .validateKeyExistsIn(schema);
    this.ksqlTopic = Objects.requireNonNull(ksqlTopic, "ksqlTopic");
    this.outputProperties = Objects.requireNonNull(outputProperties, "outputProperties");
    this.doCreateInto = doCreateInto;

    final Optional<Field> partitionBy = getPartitionByField(schema);
    if (partitionBy.isPresent()
        && !partitionBy.get().name().equals(keyField.name().orElse(null))) {
      throw new IllegalArgumentException(
          "keyField does not match partition by field. "
              + "keyField: " + keyField.name() + ", "
              + "partitionByField:" + partitionBy.get());
    }
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
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    final PlanNode source = getSource();
    final SchemaKStream schemaKStream = source.buildStream(builder);

    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId());

    final Set<Integer> rowkeyIndexes = SchemaUtil.getRowTimeRowKeyIndexes(getSchema());
    final Schema schema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(getSchema());

    final SchemaKStream<?> result = createOutputStream(
        schemaKStream,
        builder.getKsqlConfig(),
        builder.getFunctionRegistry(),
        contextStacker
    );

    final KsqlTopicSerDe ksqlTopicSerDe = getKsqlTopic()
        .getKsqlTopicSerDe();

    final Serde<GenericRow> outputRowSerde = builder.buildGenericRowSerde(
        ksqlTopicSerDe,
        schema,
        contextStacker.getQueryContext());

    result.into(
        getKafkaTopicName(),
        outputRowSerde,
        rowkeyIndexes
    );

    result.setOutputNode(this);
    return result;
  }

  @SuppressWarnings("unchecked")
  private SchemaKStream<?> createOutputStream(
      final SchemaKStream schemaKStream,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final QueryContext.Stacker contextStacker
  ) {

    if (schemaKStream instanceof SchemaKTable) {
      return schemaKStream;
    }

    final KeyField resultKeyField = KeyField.of(
        schemaKStream.getKeyField().name(),
        getKeyField().legacy()
    );

    final SchemaKStream result = new SchemaKStream(
        getSchema(),
        schemaKStream.getKstream(),
        resultKeyField,
        Collections.singletonList(schemaKStream),
        schemaKStream.getKeySerdeFactory(),
        SchemaKStream.Type.SINK,
        ksqlConfig,
        functionRegistry,
        contextStacker.getQueryContext()
    );

    final Optional<Field> partitionByField = getPartitionByField(result.getSchema());
    if (!partitionByField.isPresent()) {
      return result;
    }

    final Field field = partitionByField.get();
    return result.selectKey(field, false, contextStacker);
  }

  private Optional<Field> getPartitionByField(final Schema schema) {
    return Optional.ofNullable(outputProperties.get(DdlConfig.PARTITION_BY_PROPERTY))
        .map(Object::toString)
        .map(keyName -> SchemaUtil.getFieldByName(schema, keyName)
            .orElseThrow(() -> new KsqlException(
                "Column " + keyName + " does not exist in the result schema. "
                    + "Error in Partition By clause.")
            ));
  }

  public KsqlTopic getKsqlTopic() {
    return ksqlTopic;
  }
}