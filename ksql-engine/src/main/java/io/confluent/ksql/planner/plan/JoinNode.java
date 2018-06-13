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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.SpanExpression;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;


public class JoinNode extends PlanNode {


  public enum JoinType {
    INNER, LEFT, OUTER
  }

  private final JoinType joinType;
  private final PlanNode left;
  private final PlanNode right;
  private final Schema schema;
  private final String leftKeyFieldName;
  private final String rightKeyFieldName;

  private final String leftAlias;
  private final String rightAlias;
  private final Field keyField;
  private final SpanExpression spanExpression;
  private final DataSource.DataSourceType leftType;
  private final DataSource.DataSourceType rightType;

  public JoinNode(@JsonProperty("id") final PlanNodeId id,
                  @JsonProperty("type") final JoinType joinType,
                  @JsonProperty("left") final PlanNode left,
                  @JsonProperty("right") final PlanNode right,
                  @JsonProperty("leftKeyFieldName") final String leftKeyFieldName,
                  @JsonProperty("rightKeyFieldName") final String rightKeyFieldName,
                  @JsonProperty("leftAlias") final String leftAlias,
                  @JsonProperty("rightAlias") final String rightAlias,
                  @JsonProperty("slidingWindow") final SpanExpression spanExpression,
                  @JsonProperty("leftType") final DataSource.DataSourceType leftType,
                  @JsonProperty("rightType") final DataSource.DataSourceType rightType) {

    // TODO: Type should be derived.
    super(id);
    this.joinType = joinType;
    this.left = left;
    this.right = right;
    this.leftKeyFieldName = leftKeyFieldName;
    this.rightKeyFieldName = rightKeyFieldName;
    this.leftAlias = leftAlias;
    this.rightAlias = rightAlias;
    this.schema = buildSchema(left, right);
    this.keyField = this.schema.field((leftAlias + "." + leftKeyFieldName));
    this.spanExpression = spanExpression;
    this.leftType = leftType;
    this.rightType = rightType;
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

  public JoinType getJoinType() {
    return joinType;
  }

  public boolean isLeftJoin() {
    return joinType == JoinType.LEFT;
  }

  @Override
  public SchemaKStream buildStream(final StreamsBuilder builder,
                                   final KsqlConfig ksqlConfig,
                                   final KafkaTopicClient kafkaTopicClient,
                                   final FunctionRegistry functionRegistry,
                                   final Map<String, Object> props,
                                   final SchemaRegistryClient schemaRegistryClient) {

    ensureMatchingPartitionCounts(kafkaTopicClient);

    JoinHelper joinHelper = new JoinHelper(builder,
                                           ksqlConfig,
                                           kafkaTopicClient,
                                           functionRegistry,
                                           props,
                                           schemaRegistryClient,
                                           this);

    if (leftType.isStream() && rightType.isStream()) {
      return joinHelper.doStreamToStreamJoin();
    }

    if (leftType.isStream() && rightType.isTable()) {
      return joinHelper.doStreamToTableJoin();
    }

    if (leftType.isTable() && rightType.isTable()) {
      return joinHelper.doTableToTableJoin();
    }

    throw new KsqlException("Invalid join operands provided. left: " + leftType + "; right: "
                            + rightType + ".");

  }

  @Override
  protected int getPartitions(KafkaTopicClient kafkaTopicClient) {
    return right.getPartitions(kafkaTopicClient);
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


  private void ensureMatchingPartitionCounts(final KafkaTopicClient kafkaTopicClient) {
    final int leftPartitions = left.getPartitions(kafkaTopicClient);
    final int rightPartitions = right.getPartitions(kafkaTopicClient);
    if (leftPartitions != rightPartitions) {
      throw new KsqlException("Can't join " + leftType.getKqlType() + " with "
                              + rightType.getKqlType() + " since the number of partitions don't "
                              + "match. " + leftType.getKqlType() + " partitions = "
                              + leftPartitions + "; " + rightType.getKqlType() + " partitions = "
                              + rightPartitions + ". Please repartition either one so that the "
                              + "number of partitions match.");
    }
  }

  private static class JoinHelper {
    private final StreamsBuilder builder;
    private final KsqlConfig ksqlConfig;
    private final KafkaTopicClient kafkaTopicClient;
    private final FunctionRegistry functionRegistry;
    private final Map<String, Object> props;
    private final SchemaRegistryClient schemaRegistryClient;
    private final JoinNode joinNode;

    JoinHelper(final StreamsBuilder builder,
               final KsqlConfig ksqlConfig,
               final KafkaTopicClient kafkaTopicClient,
               final FunctionRegistry functionRegistry,
               final Map<String, Object> props,
               final SchemaRegistryClient schemaRegistryClient,
               final JoinNode joinNode) {

      this.builder = builder;
      this.ksqlConfig = ksqlConfig;
      this.kafkaTopicClient = kafkaTopicClient;
      this.functionRegistry = functionRegistry;
      this.props = props;
      this.schemaRegistryClient = schemaRegistryClient;
      this.joinNode = joinNode;

    }

    SchemaKStream doStreamToStreamJoin() {
      final SchemaKStream leftStream = buildStream(joinNode.getLeft(),
                                                   joinNode.getLeftKeyFieldName());
      final SchemaKStream rightStream = buildStream(joinNode.getRight(),
                                                    joinNode.getRightKeyFieldName());

      if (joinNode.spanExpression == null) {
        throw new KsqlException("Stream-Stream joins must have a SPAN clause specified. None was "
                                + "provided. To learn about how to specify a SPAN clause with a "
                                + "stream-stream join, please visit: https://docs.confluent"
                                + ".io/current/ksql/docs/syntax-reference.html"
                                + "#create-stream-as-select");
      }

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(rightStream,
                                     joinNode.schema,
                                     getJoinKey(joinNode.leftAlias,
                                                leftStream.getKeyField().name()),
                                     joinNode.spanExpression.joinWindow(),
                                     getSerDeForNode(joinNode.left),
                                     getSerDeForNode(joinNode.right));
        case OUTER:
          return leftStream.outerJoin(rightStream,
                                      joinNode.schema,
                                      getJoinKey(joinNode.leftAlias,
                                                 leftStream.getKeyField().name()),
                                      joinNode.spanExpression.joinWindow(),
                                      getSerDeForNode(joinNode.left),
                                      getSerDeForNode(joinNode.right));
        case INNER:
          return leftStream.join(rightStream,
                                      joinNode.schema,
                                      getJoinKey(joinNode.leftAlias,
                                                 leftStream.getKeyField().name()),
                                      joinNode.spanExpression.joinWindow(),
                                      getSerDeForNode(joinNode.left),
                                      getSerDeForNode(joinNode.right));
        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }
    }

    SchemaKStream doStreamToTableJoin() {
      final SchemaKStream leftStream = buildStream(joinNode.getLeft(),
                                                   joinNode.getLeftKeyFieldName());
      final SchemaKTable rightTable = buildTable(joinNode.getRight());

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(rightTable,
                                     joinNode.schema,
                                     getJoinKey(joinNode.leftAlias,
                                                leftStream.getKeyField().name()),
                                     getSerDeForNode(joinNode.left));

        case INNER:
          return leftStream.join(rightTable,
                                 joinNode.schema,
                                 getJoinKey(joinNode.leftAlias,
                                            leftStream.getKeyField().name()),
                                 getSerDeForNode(joinNode.left));
        case OUTER:
          throw new KsqlException("Outer joins between streams and tables (stream: left, "
                                  + "table: right) are not supported.");

        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }
    }

    SchemaKTable doTableToTableJoin() {
      final SchemaKTable leftTable = buildTable(joinNode.getLeft());
      final SchemaKTable rightTable = buildTable(joinNode.getRight());

      switch (joinNode.joinType) {
        case LEFT:
          return leftTable.leftJoin(rightTable, joinNode.schema,
                                    getJoinKey(joinNode.leftAlias, leftTable.getKeyField().name()));
        case INNER:
          return leftTable.join(rightTable, joinNode.schema,
                                getJoinKey(joinNode.leftAlias, leftTable.getKeyField().name()));
        case OUTER:
          return leftTable.outerJoin(rightTable, joinNode.schema,
                                     getJoinKey(joinNode.leftAlias,
                                                leftTable.getKeyField().name()));
        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }

    }

    private Field getJoinKey(final String alias, final String keyFieldName) {
      return joinNode.schema.field(alias + "." + keyFieldName);
    }


    private SchemaKStream buildStream(final PlanNode node, final String keyFieldName) {
      return maybeRePartitionByKey(node.buildStream(builder, ksqlConfig, kafkaTopicClient,
                                                    functionRegistry, props,
                                                    schemaRegistryClient),
                                   node.getKeyField(),
                                   keyFieldName);
    }

    private SchemaKTable buildTable(final PlanNode node) {

      Map<String, Object> joinTableProps = new HashMap<>(props);
      joinTableProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      final SchemaKStream schemaKStream = node.buildStream(
          builder,
          ksqlConfig,
          kafkaTopicClient,
          functionRegistry,
          joinTableProps, schemaRegistryClient);

      if (!(schemaKStream instanceof SchemaKTable)) {
        throw new KsqlException("Expected to find a Table, found a stream instead");
      }

      return (SchemaKTable) schemaKStream;
    }

    private SchemaKStream maybeRePartitionByKey(final SchemaKStream stream,
                                                final Field currentKey,
                                                final String targetKey) {
      if (currentKey != null && currentKey.name().equals(targetKey)) {
        // if the current key is defined and the name matches the column we are joining on, then
        // don't repartition.
        return stream;
      }
      final Schema schema = stream.getSchema();
      final Field field =
          SchemaUtil
              .getFieldByName(schema,
                              targetKey).orElseThrow(() -> new KsqlException("couldn't find "
                                                                                + "key field: "
                                                                                + targetKey
                                                                                + " in schema:"
                                                                                + schema)
          );
      return stream.selectKey(field, true);
    }

    private Serde<GenericRow> getSerDeForNode(final PlanNode node) {
      if (!(node instanceof StructuredDataSourceNode)) {
        throw new KsqlException("The source for Join must be a primitive data source (Stream or "
                                + "Table)");
      }
      final StructuredDataSourceNode dataSourceNode = (StructuredDataSourceNode) node;
      return dataSourceNode
          .getStructuredDataSource()
          .getKsqlTopic()
          .getKsqlTopicSerDe()
          .getGenericRowSerde(dataSourceNode.getSchema(), ksqlConfig, false, schemaRegistryClient);
    }
  }

}
