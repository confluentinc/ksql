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

import com.google.common.collect.ImmutableMap;

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
import java.util.function.Supplier;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
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
  private final WithinExpression withinExpression;
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
                  @JsonProperty("within") final WithinExpression withinExpression,
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
    this.withinExpression = withinExpression;
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

    JoinerFactory joinerFactory = new JoinerFactory(builder,
                                                    ksqlConfig,
                                                    kafkaTopicClient,
                                                    functionRegistry,
                                                    props,
                                                    schemaRegistryClient,
                                                    this);

    return joinerFactory.getJoiner(leftType, rightType).join();
  }

  @Override
  protected int getPartitions(KafkaTopicClient kafkaTopicClient) {
    return right.getPartitions(kafkaTopicClient);
  }

  private void ensureMatchingPartitionCounts(final KafkaTopicClient kafkaTopicClient) {
    final int leftPartitions = left.getPartitions(kafkaTopicClient);
    final int rightPartitions = right.getPartitions(kafkaTopicClient);

    if (leftPartitions != rightPartitions) {
      throw new KsqlException("Can't join " + getSourceName(left) + " with "
                              + getSourceName(right) + " since the number of partitions don't "
                              + "match. " + getSourceName(left) + " partitions = "
                              + leftPartitions + "; " + getSourceName(right) + " partitions = "
                              + rightPartitions + ". Please repartition either one so that the "
                              + "number of partitions match.");
    }
  }

  private String getSourceName(PlanNode node) {
    if (!(node instanceof StructuredDataSourceNode)) {
      throw new RuntimeException("The source for a join must be a Stream or a Table.");
    }
    StructuredDataSourceNode dataSource = (StructuredDataSourceNode) node;
    return dataSource.getStructuredDataSource().getName();
  }

  private static class JoinerFactory {
    private final Map<
        Pair<DataSource.DataSourceType, DataSource.DataSourceType>,
        Supplier<Joiner>> joinerMap;

    JoinerFactory(final StreamsBuilder builder,
                  final KsqlConfig ksqlConfig,
                  final KafkaTopicClient kafkaTopicClient,
                  final FunctionRegistry functionRegistry,
                  final Map<String, Object> props,
                  final SchemaRegistryClient schemaRegistryClient,
                  final JoinNode joinNode) {
      this.joinerMap = ImmutableMap.of(
          new Pair<>(DataSource.DataSourceType.KSTREAM, DataSource.DataSourceType.KSTREAM),
          () -> new StreamToStreamJoiner(builder, ksqlConfig, kafkaTopicClient, functionRegistry,
                                         props, schemaRegistryClient, joinNode),
          new Pair<>(DataSource.DataSourceType.KSTREAM, DataSource.DataSourceType.KTABLE),
          () -> new StreamToTableJoiner(builder, ksqlConfig, kafkaTopicClient, functionRegistry,
                                        props, schemaRegistryClient, joinNode),
          new Pair<>(DataSource.DataSourceType.KTABLE, DataSource.DataSourceType.KTABLE),
          () -> new TableToTableJoiner(builder, ksqlConfig, kafkaTopicClient, functionRegistry,
                                       props, schemaRegistryClient, joinNode)
      );
    }

    public Joiner getJoiner(DataSource.DataSourceType leftType,
                            DataSource.DataSourceType rightType) {

      return joinerMap.getOrDefault(new Pair<>(leftType, rightType), () -> {
        throw new KsqlException("Join between invalid operands requested: left type: "
                                + leftType + ", right type: " + rightType);
      }).get();
    }
  }

  private abstract static class Joiner {
    protected final StreamsBuilder builder;
    protected final KsqlConfig ksqlConfig;
    protected final KafkaTopicClient kafkaTopicClient;
    protected final FunctionRegistry functionRegistry;
    protected final Map<String, Object> props;
    protected final SchemaRegistryClient schemaRegistryClient;
    protected final JoinNode joinNode;

    protected Joiner(final StreamsBuilder builder,
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

    public abstract SchemaKStream join();

    protected SchemaKStream buildStream(final PlanNode node, final String keyFieldName) {
      return maybeRePartitionByKey(node.buildStream(builder, ksqlConfig, kafkaTopicClient,
                                                    functionRegistry, props,
                                                    schemaRegistryClient),
                                   keyFieldName);
    }


    protected SchemaKTable buildTable(final PlanNode node) {

      final Map<String, Object> joinTableProps = new HashMap<>(props);
      joinTableProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      final SchemaKStream schemaKStream = node.buildStream(
          builder,
          ksqlConfig,
          kafkaTopicClient,
          functionRegistry,
          joinTableProps, schemaRegistryClient);

      if (!(schemaKStream instanceof SchemaKTable)) {
        throw new RuntimeException("Expected to find a Table, found a stream instead.");
      }

      return (SchemaKTable) schemaKStream;
    }

    protected SchemaKStream maybeRePartitionByKey(final SchemaKStream stream,
                                                  final String targetKey) {
      final Schema schema = stream.getSchema();
      final Field field =
          SchemaUtil
              .getFieldByName(schema,
                              targetKey).orElseThrow(() -> new KsqlException("couldn't find "
                                                                             + "key field: "
                                                                             + targetKey
                                                                             + " in schema")
          );
      return stream.selectKey(field, true);
    }

    protected Serde<GenericRow> getSerDeForNode(final PlanNode node) {
      if (!(node instanceof StructuredDataSourceNode)) {
        throw new KsqlException("The source for Join must be a primitive data source (Stream or "
                                + "Table).");
      }
      final StructuredDataSourceNode dataSourceNode = (StructuredDataSourceNode) node;
      return dataSourceNode
          .getStructuredDataSource()
          .getKsqlTopic()
          .getKsqlTopicSerDe()
          .getGenericRowSerde(dataSourceNode.getSchema(), ksqlConfig, false, schemaRegistryClient);
    }

    protected Field getJoinKey(final String alias, final String keyFieldName) {
      return joinNode.schema.field(alias + "." + keyFieldName);
    }

  }

  private static class StreamToStreamJoiner extends Joiner {

    StreamToStreamJoiner(final StreamsBuilder builder,
                         final KsqlConfig ksqlConfig,
                         final KafkaTopicClient kafkaTopicClient,
                         final FunctionRegistry functionRegistry,
                         final Map<String, Object> props,
                         final SchemaRegistryClient schemaRegistryClient,
                         final JoinNode joinNode) {
      super(builder, ksqlConfig, kafkaTopicClient, functionRegistry, props, schemaRegistryClient,
            joinNode);
    }

    @Override
    public SchemaKStream join() {
      if (joinNode.withinExpression == null) {
        throw new KsqlException("Stream-Stream joins must have a WITHIN clause specified. None was "
                                + "provided. To learn about how to specify a WITHIN clause with a "
                                + "stream-stream join, please visit: https://docs.confluent"
                                + ".io/current/ksql/docs/syntax-reference.html"
                                + "#create-stream-as-select");
      }

      final SchemaKStream leftStream = buildStream(joinNode.getLeft(),
                                                   joinNode.getLeftKeyFieldName());
      final SchemaKStream rightStream = buildStream(joinNode.getRight(),
                                                    joinNode.getRightKeyFieldName());

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(rightStream,
                                     joinNode.schema,
                                     getJoinKey(joinNode.leftAlias,
                                                leftStream.getKeyField().name()),
                                     joinNode.withinExpression.joinWindow(),
                                     getSerDeForNode(joinNode.left),
                                     getSerDeForNode(joinNode.right));
        case OUTER:
          return leftStream.outerJoin(rightStream,
                                      joinNode.schema,
                                      getJoinKey(joinNode.leftAlias,
                                                 leftStream.getKeyField().name()),
                                      joinNode.withinExpression.joinWindow(),
                                      getSerDeForNode(joinNode.left),
                                      getSerDeForNode(joinNode.right));
        case INNER:
          return leftStream.join(rightStream,
                                 joinNode.schema,
                                 getJoinKey(joinNode.leftAlias,
                                            leftStream.getKeyField().name()),
                                 joinNode.withinExpression.joinWindow(),
                                 getSerDeForNode(joinNode.left),
                                 getSerDeForNode(joinNode.right));
        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }
    }
  }

  private static class StreamToTableJoiner extends Joiner {

    StreamToTableJoiner(final StreamsBuilder builder,
                        final KsqlConfig ksqlConfig,
                        final KafkaTopicClient kafkaTopicClient,
                        final FunctionRegistry functionRegistry,
                        final Map<String, Object> props,
                        final SchemaRegistryClient schemaRegistryClient,
                        final JoinNode joinNode) {
      super(builder, ksqlConfig, kafkaTopicClient, functionRegistry, props, schemaRegistryClient,
            joinNode);
    }

    @Override
    public SchemaKStream join() {
      if (joinNode.withinExpression != null) {
        throw new KsqlException("A window definition was provided for a Stream-Table join. These "
                                + "joins are not windowed. Please drop the window definition (ie."
                                + " the WITHIN clause) and try to execute your join again.");
      }

      final SchemaKTable rightTable = buildTable(joinNode.getRight());
      final SchemaKStream leftStream = buildStream(joinNode.getLeft(),
                                                   joinNode.getLeftKeyFieldName());

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
          throw new KsqlException("Full outer joins between streams and tables (stream: left, "
                                  + "table: right) are not supported.");

        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }
    }
  }

  private static class TableToTableJoiner extends Joiner {

    TableToTableJoiner(final StreamsBuilder builder,
                        final KsqlConfig ksqlConfig,
                        final KafkaTopicClient kafkaTopicClient,
                        final FunctionRegistry functionRegistry,
                        final Map<String, Object> props,
                        final SchemaRegistryClient schemaRegistryClient,
                        final JoinNode joinNode) {
      super(builder, ksqlConfig, kafkaTopicClient, functionRegistry, props, schemaRegistryClient,
            joinNode);
    }

    @Override
    public SchemaKTable join() {
      if (joinNode.withinExpression != null) {
        throw new KsqlException("A window definition was provided for a Table-Table join. These "
                                + "joins are not windowed. Please drop the window definition "
                                + "(i.e. the WITHIN clause) and try to execute your Table-Table "
                                + "join again.");
      }

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
  }
}
