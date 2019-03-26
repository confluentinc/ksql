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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;


public class JoinNode extends PlanNode {

  public enum JoinType {
    INNER, LEFT, OUTER
  }

  private static final String LEFT_SERDE_CONTEXT_NAME = "left";
  private static final String RIGHT_SERDE_CONTEXT_NAME = "right";

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

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public JoinNode(
      @JsonProperty("id") final PlanNodeId id,
      @JsonProperty("type") final JoinType joinType,
      @JsonProperty("left") final PlanNode left,
      @JsonProperty("right") final PlanNode right,
      @JsonProperty("leftKeyFieldName") final String leftKeyFieldName,
      @JsonProperty("rightKeyFieldName") final String rightKeyFieldName,
      @JsonProperty("leftAlias") final String leftAlias,
      @JsonProperty("rightAlias") final String rightAlias,
      @JsonProperty("within") final WithinExpression withinExpression,
      @JsonProperty("leftType") final DataSource.DataSourceType leftType,
      @JsonProperty("rightType") final DataSource.DataSourceType rightType
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    super(id, (leftType == DataSourceType.KTABLE && rightType == DataSourceType.KTABLE)
        ? DataSourceType.KTABLE
        : DataSourceType.KSTREAM);
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

    final Schema leftSchema = left.getSchema();
    final Schema rightSchema = right.getSchema();

    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    for (final Field field : leftSchema.fields()) {
      final String fieldName = leftAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }

    for (final Field field : rightSchema.fields()) {
      final String fieldName = rightAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }
    return schemaBuilder.build();
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public Optional<Field> getKeyField() {
    return Optional.of(keyField);
  }

  @Override
  public List<PlanNode> getSources() {
    return Arrays.asList(left, right);
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
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

  JoinType getJoinType() {
    return joinType;
  }

  public boolean isLeftJoin() {
    return joinType == JoinType.LEFT;
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {

    ensureMatchingPartitionCounts(builder.getServiceContext().getTopicClient());

    final JoinerFactory joinerFactory = new JoinerFactory(
        builder,
        this,
        builder.buildNodeContext(getId()));

    return joinerFactory.getJoiner(leftType, rightType).join();
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
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

  private static String getSourceName(final PlanNode node) {
    if (!(node instanceof StructuredDataSourceNode)) {
      throw new RuntimeException("The source for a join must be a Stream or a Table.");
    }
    final StructuredDataSourceNode dataSource = (StructuredDataSourceNode) node;
    return dataSource.getStructuredDataSource().getName();
  }

  private static class JoinerFactory {
    private final Map<
        Pair<DataSource.DataSourceType, DataSource.DataSourceType>,
        Supplier<Joiner>> joinerMap;

    JoinerFactory(
        final KsqlQueryBuilder builder,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      this.joinerMap = ImmutableMap.of(
          new Pair<>(DataSource.DataSourceType.KSTREAM, DataSource.DataSourceType.KSTREAM),
          () -> new StreamToStreamJoiner(
              builder,
              joinNode,
              contextStacker),
          new Pair<>(DataSource.DataSourceType.KSTREAM, DataSource.DataSourceType.KTABLE),
          () -> new StreamToTableJoiner(
              builder,
              joinNode,
              contextStacker),
          new Pair<>(DataSource.DataSourceType.KTABLE, DataSource.DataSourceType.KTABLE),
          () -> new TableToTableJoiner(
              builder,
              joinNode,
              contextStacker)
      );
    }

    Joiner getJoiner(final DataSource.DataSourceType leftType,
        final DataSource.DataSourceType rightType) {

      return joinerMap.getOrDefault(new Pair<>(leftType, rightType), () -> {
        throw new KsqlException("Join between invalid operands requested: left type: "
                                + leftType + ", right type: " + rightType);
      }).get();
    }
  }

  private abstract static class Joiner<K> {

    final KsqlQueryBuilder builder;
    final JoinNode joinNode;
    final QueryContext.Stacker contextStacker;

    Joiner(
        final KsqlQueryBuilder builder,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      this.builder = Objects.requireNonNull(builder, "builder");
      this.joinNode = Objects.requireNonNull(joinNode, "joinNode");
      this.contextStacker = Objects.requireNonNull(contextStacker, "contextStacker");
    }

    public abstract SchemaKStream<K> join();

    protected SchemaKStream<K> buildStream(final PlanNode node, final String keyFieldName) {

      return maybeRePartitionByKey(
          node.buildStream(builder),
          keyFieldName,
          contextStacker);
    }

    @SuppressWarnings("unchecked")
    protected SchemaKTable<K> buildTable(
        final PlanNode node,
        final String keyFieldName,
        final String tableName
    ) {
      final SchemaKStream<?> schemaKStream = node.buildStream(
          builder.withKsqlConfig(builder.getKsqlConfig()
                  .cloneWithPropertyOverwrite(Collections.singletonMap(
                      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")))
      );

      if (!(schemaKStream instanceof SchemaKTable)) {
        throw new RuntimeException("Expected to find a Table, found a stream instead.");
      }

      if (schemaKStream.getKeyField().isPresent()
          && !keyFieldName.equals(SchemaUtil.ROWKEY_NAME)
          && !SchemaUtil.matchFieldName(schemaKStream.getKeyField().get(), keyFieldName)) {
        throw new KsqlException(
            String.format(
                "Source table (%s) key column (%s) "
                    + "is not the column used in the join criteria (%s).",
                tableName,
                schemaKStream.getKeyField().get().name(),
                keyFieldName
            )
        );
      }

      return (SchemaKTable) schemaKStream;
    }

    @SuppressWarnings("unchecked")
    static <K> SchemaKStream<K> maybeRePartitionByKey(
        final SchemaKStream stream,
        final String targetKey,
        final QueryContext.Stacker contextStacker) {
      final Schema schema = stream.getSchema();
      final Field field =
          SchemaUtil
              .getFieldByName(schema,
                              targetKey).orElseThrow(() -> new KsqlException("couldn't find "
                                                                             + "key field: "
                                                                             + targetKey
                                                                             + " in schema")
          );
      return stream.selectKey(field, true, contextStacker);
    }

    Serde<GenericRow> getSerDeForNode(
        final PlanNode node,
        final QueryContext.Stacker contextStacker) {
      if (!(node instanceof StructuredDataSourceNode)) {
        throw new KsqlException("The source for Join must be a primitive data source (Stream or "
                                + "Table).");
      }
      final StructuredDataSourceNode dataSourceNode = (StructuredDataSourceNode) node;
      final KsqlTopicSerDe ksqlTopicSerDe = dataSourceNode
          .getStructuredDataSource()
          .getKsqlTopic()
          .getKsqlTopicSerDe();

      return builder.buildGenericRowSerde(
          ksqlTopicSerDe,
          dataSourceNode.getSchema(),
          contextStacker.getQueryContext()
      );
    }

    Field getJoinKey(final String alias, final String keyFieldName) {
      return joinNode.schema.field(alias + "." + keyFieldName);
    }
  }

  private static final class StreamToStreamJoiner<K> extends Joiner<K> {

    private StreamToStreamJoiner(
        final KsqlQueryBuilder builder,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      super(builder, joinNode, contextStacker);
    }

    @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
    @Override
    public SchemaKStream<K> join() {
      if (joinNode.withinExpression == null) {
        throw new KsqlException("Stream-Stream joins must have a WITHIN clause specified. None was "
                                + "provided. To learn about how to specify a WITHIN clause with a "
                                + "stream-stream join, please visit: https://docs.confluent"
                                + ".io/current/ksql/docs/syntax-reference.html"
                                + "#create-stream-as-select");
      }

      final SchemaKStream<K> leftStream = buildStream(
          joinNode.getLeft(), joinNode.getLeftKeyFieldName());

      final SchemaKStream<K> rightStream = buildStream(
          joinNode.getRight(), joinNode.getRightKeyFieldName());

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(rightStream,
                                     joinNode.schema,
                                     getJoinKey(joinNode.leftAlias,
                                                leftStream.getKeyField().get().name()),
                                     joinNode.withinExpression.joinWindow(),
                                     getSerDeForNode(
                                         joinNode.left,
                                         contextStacker.push(LEFT_SERDE_CONTEXT_NAME)),
                                     getSerDeForNode(
                                         joinNode.right,
                                         contextStacker.push(RIGHT_SERDE_CONTEXT_NAME)),
              contextStacker);
        case OUTER:
          return leftStream.outerJoin(rightStream,
                                      joinNode.schema,
                                      getJoinKey(joinNode.leftAlias,
                                                 leftStream.getKeyField().get().name()),
                                      joinNode.withinExpression.joinWindow(),
                                      getSerDeForNode(
                                          joinNode.left,
                                          contextStacker.push(LEFT_SERDE_CONTEXT_NAME)),
                                      getSerDeForNode(
                                          joinNode.right,
                                          contextStacker.push(RIGHT_SERDE_CONTEXT_NAME)),
              contextStacker);
        case INNER:
          return leftStream.join(rightStream,
                                 joinNode.schema,
                                 getJoinKey(joinNode.leftAlias,
                                            leftStream.getKeyField().get().name()),
                                 joinNode.withinExpression.joinWindow(),
                                 getSerDeForNode(
                                     joinNode.left,
                                     contextStacker.push(LEFT_SERDE_CONTEXT_NAME)),
                                 getSerDeForNode(
                                     joinNode.right,
                                     contextStacker.push(RIGHT_SERDE_CONTEXT_NAME)),
              contextStacker);
        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }
    }
  }

  private static final class StreamToTableJoiner<K> extends Joiner<K> {

    private StreamToTableJoiner(
        final KsqlQueryBuilder builder,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      super(builder, joinNode, contextStacker);
    }

    @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
    @Override
    public SchemaKStream<K> join() {
      if (joinNode.withinExpression != null) {
        throw new KsqlException("A window definition was provided for a Stream-Table join. These "
                                + "joins are not windowed. Please drop the window definition (ie."
                                + " the WITHIN clause) and try to execute your join again.");
      }

      final SchemaKTable<K> rightTable = buildTable(joinNode.getRight(),
                                                 joinNode.getRightKeyFieldName(),
                                                 joinNode.getRightAlias());
      final SchemaKStream<K> leftStream = buildStream(joinNode.getLeft(),
                                                   joinNode.getLeftKeyFieldName());

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(rightTable,
                                     joinNode.schema,
                                     getJoinKey(joinNode.leftAlias,
                                                leftStream.getKeyField().get().name()),
                                     getSerDeForNode(
                                         joinNode.left,
                                         contextStacker.push(LEFT_SERDE_CONTEXT_NAME)),
              contextStacker);

        case INNER:
          return leftStream.join(rightTable,
                                 joinNode.schema,
                                 getJoinKey(joinNode.leftAlias,
                                            leftStream.getKeyField().get().name()),
                                 getSerDeForNode(
                                     joinNode.left,
                                     contextStacker.push(LEFT_SERDE_CONTEXT_NAME)),
              contextStacker);
        case OUTER:
          throw new KsqlException("Full outer joins between streams and tables (stream: left, "
                                  + "table: right) are not supported.");

        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }
    }
  }

  private static final class TableToTableJoiner<K> extends Joiner<K> {

    TableToTableJoiner(
        final KsqlQueryBuilder builder,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      super(builder, joinNode, contextStacker);
    }

    @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
    @Override
    public SchemaKTable<K> join() {
      if (joinNode.withinExpression != null) {
        throw new KsqlException("A window definition was provided for a Table-Table join. These "
                                + "joins are not windowed. Please drop the window definition "
                                + "(i.e. the WITHIN clause) and try to execute your Table-Table "
                                + "join again.");
      }

      final SchemaKTable<K> leftTable = buildTable(joinNode.getLeft(),
                                                joinNode.getLeftKeyFieldName(),
                                                joinNode.getLeftAlias());
      final SchemaKTable<K> rightTable = buildTable(joinNode.getRight(),
                                                 joinNode.getRightKeyFieldName(),
                                                 joinNode.getRightAlias());

      switch (joinNode.joinType) {
        case LEFT:
          return leftTable.leftJoin(
              rightTable,
              joinNode.schema,
              getJoinKey(joinNode.leftAlias, leftTable.getKeyField().get().name()),
              contextStacker);
        case INNER:
          return leftTable.join(
              rightTable,
              joinNode.schema,
              getJoinKey(joinNode.leftAlias, leftTable.getKeyField().get().name()),
              contextStacker);
        case OUTER:
          return leftTable.outerJoin(
              rightTable,
              joinNode.schema,
              getJoinKey(joinNode.leftAlias, leftTable.getKeyField().get().name()),
              contextStacker);
        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }
    }
  }
}
