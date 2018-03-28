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

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

public class JoinNode extends PlanNode {


  public enum Type {
    CROSS, INNER, LEFT, RIGHT, FULL, IMPLICIT
  }

  private final Type type;
  private final PlanNode left;
  private final PlanNode right;
  private final Schema schema;
  private final String leftKeyFieldName;
  private final String rightKeyFieldName;

  private final String leftAlias;
  private final String rightAlias;
  private final Field keyField;

  public JoinNode(@JsonProperty("id") final PlanNodeId id,
                  @JsonProperty("type") final Type type,
                  @JsonProperty("left") final PlanNode left,
                  @JsonProperty("right") final PlanNode right,
                  @JsonProperty("leftKeyFieldName") final String leftKeyFieldName,
                  @JsonProperty("rightKeyFieldName") final String rightKeyFieldName,
                  @JsonProperty("leftAlias") final String leftAlias,
                  @JsonProperty("rightAlias") final String rightAlias) {

    // TODO: Type should be derived.
    super(id);
    this.type = type;
    this.left = left;
    this.right = right;
    this.leftKeyFieldName = leftKeyFieldName;
    this.rightKeyFieldName = rightKeyFieldName;
    this.leftAlias = leftAlias;
    this.rightAlias = rightAlias;
    this.schema = buildSchema(left, right);
    this.keyField = this.schema.field((leftAlias + "." + leftKeyFieldName));
  }

  private Schema buildSchema(final PlanNode left, final PlanNode right) {

    Schema leftSchema = left.getSchema();
    Schema rightSchema = right.getSchema();

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    for (Field field : leftSchema.fields()) {
      String fieldName = leftAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }

    for (Field field : rightSchema.fields()) {
      String fieldName = rightAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }
    return schemaBuilder.build();
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public Field getKeyField() {
    return this.keyField;
  }

  @Override
  public List<PlanNode> getSources() {
    return Arrays.asList(left, right);
  }

  @Override
  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitJoin(this, context);
  }

  public PlanNode getLeft() {
    return left;
  }

  public PlanNode getRight() {
    return right;
  }

  public String getLeftKeyFieldName() {
    return leftKeyFieldName;
  }

  public String getRightKeyFieldName() {
    return rightKeyFieldName;
  }

  public String getLeftAlias() {
    return leftAlias;
  }

  public String getRightAlias() {
    return rightAlias;
  }

  public Type getType() {
    return type;
  }

  public boolean isLeftJoin() {
    return type == Type.LEFT;
  }

  @Override
  public SchemaKStream buildStream(final StreamsBuilder builder,
                                   final KsqlConfig ksqlConfig,
                                   final KafkaTopicClient kafkaTopicClient,
                                   final FunctionRegistry functionRegistry,
                                   final Map<String, Object> props,
                                   final SchemaRegistryClient schemaRegistryClient) {
    if (!isLeftJoin()) {
      throw new KsqlException("Join type is not supported yet: " + getType());
    }

    final SchemaKTable table = tableForJoin(builder,
        ksqlConfig,
        kafkaTopicClient,
        functionRegistry,
        props, schemaRegistryClient);

    final SchemaKStream stream = streamForJoin(getLeft().buildStream(builder,
        ksqlConfig,
        kafkaTopicClient,
        functionRegistry,
        props, schemaRegistryClient), getLeftKeyFieldName(), kafkaTopicClient);

    final KsqlTopicSerDe joinSerDe = getResultTopicSerde(this);
    return stream.leftJoin(table,
        getSchema(),
        getSchema().field(getLeftAlias() + "." + stream.getKeyField().name()),
        joinSerDe, ksqlConfig);

  }

  // package private for test
  SchemaKTable tableForJoin(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> props,
      final SchemaRegistryClient schemaRegistryClient) {

    Map<String, Object> joinTableProps = new HashMap<>(props);
    joinTableProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final SchemaKStream schemaKStream = right.buildStream(
        builder,
        ksqlConfig,
        kafkaTopicClient,
        functionRegistry,
        joinTableProps, schemaRegistryClient);
    if (!(schemaKStream instanceof SchemaKTable)) {
      throw new KsqlException("Unsupported Join. Only stream-table joins are supported, but was "
          + getLeft() + "-" + getRight());
    }

    return (SchemaKTable) schemaKStream;
  }


  private KsqlTopicSerDe getResultTopicSerde(final PlanNode node) {
    if (node instanceof StructuredDataSourceNode) {
      StructuredDataSourceNode structuredDataSourceNode = (StructuredDataSourceNode) node;
      return structuredDataSourceNode.getStructuredDataSource().getKsqlTopic().getKsqlTopicSerDe();
    } else if (node instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) node;

      return getResultTopicSerde(joinNode.getLeft());
    } else {
      return getResultTopicSerde(node.getSources().get(0));
    }
  }

  @Override
  protected int getPartitions(KafkaTopicClient kafkaTopicClient) {
    return right.getPartitions(kafkaTopicClient);
  }

  private SchemaKStream streamForJoin(final SchemaKStream stream, final String leftKeyFieldName,
                                      KafkaTopicClient kafkaTopicClient) {
    if (left.getPartitions(kafkaTopicClient) != getPartitions(kafkaTopicClient)) {
      throw new KsqlException(
          "Stream and Table have different number of partitions. Either the stream or the table"
              + "must be repartitioned such that both have the same number of partitions.");
    }
    final Field field = SchemaUtil.getFieldByName(stream.getSchema(),
        leftKeyFieldName).orElseThrow(() -> new KsqlException("couldn't find key field: "
        + leftKeyFieldName
        + " in schema:"
        + schema));
    return stream.selectKey(field, true);
  }
}
