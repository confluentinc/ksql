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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.JoinParamsFactory;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.KafkaTopicClient;
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

public class JoinNode extends PlanNode {

  public enum JoinType {
    INNER, LEFT, OUTER
  }

  private final JoinType joinType;
  private final DataSourceNode left;
  private final DataSourceNode right;
  private final LogicalSchema schema;
  private final ColumnRef leftJoinFieldName;
  private final ColumnRef rightJoinFieldName;
  private final KeyField keyField;
  private final Optional<WithinExpression> withinExpression;
  private final ImmutableList<SelectExpression> selectExpressions;

  public JoinNode(
      final PlanNodeId id,
      final List<SelectExpression> selectExpressions,
      final JoinType joinType,
      final DataSourceNode left,
      final DataSourceNode right,
      final ColumnRef leftJoinFieldName,
      final ColumnRef rightJoinFieldName,
      final Optional<WithinExpression> withinExpression
  ) {
    super(id, calculateSinkType(left, right));
    this.joinType = Objects.requireNonNull(joinType, "joinType");
    this.left = Objects.requireNonNull(left, "left");
    this.right = Objects.requireNonNull(right, "right");
    this.leftJoinFieldName = Objects.requireNonNull(leftJoinFieldName, "leftJoinFieldName");
    this.rightJoinFieldName = Objects.requireNonNull(rightJoinFieldName, "rightJoinFieldName");
    this.withinExpression = Objects.requireNonNull(withinExpression, "withinExpression");
    this.selectExpressions = ImmutableList
        .copyOf(Objects.requireNonNull(selectExpressions, "selectExpressions"));

    final Column leftKeyCol = validateSchemaColumn(leftJoinFieldName, left.getSchema());
    validateSchemaColumn(rightJoinFieldName, right.getSchema());

    this.keyField = joinType == JoinType.OUTER
        ? KeyField.none() // Both source key columns can be null, hence neither can be the keyField
        : left.getSchema().isKeyColumn(leftKeyCol.name())
            ? left.getKeyField()
            : KeyField.of(leftKeyCol.ref());

    this.schema = buildJoinSchema(left, leftJoinFieldName, right, rightJoinFieldName);
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public List<PlanNode> getSources() {
    return Arrays.asList(left, right);
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitJoin(this, context);
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
  }

  public DataSourceNode getLeft() {
    return left;
  }

  public DataSourceNode getRight() {
    return right;
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {

    ensureMatchingPartitionCounts(builder.getServiceContext().getTopicClient());

    final JoinerFactory joinerFactory = new JoinerFactory(
        builder,
        this,
        builder.buildNodeContext(getId().toString()));

    return joinerFactory.getJoiner(left.getDataSourceType(), right.getDataSourceType()).join();
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return right.getPartitions(kafkaTopicClient);
  }

  private void ensureMatchingPartitionCounts(final KafkaTopicClient kafkaTopicClient) {
    final int leftPartitions = left.getPartitions(kafkaTopicClient);
    final int rightPartitions = right.getPartitions(kafkaTopicClient);

    if (leftPartitions != rightPartitions) {
      throw new KsqlException(
          "Can't join " + getSourceName(left) + " with "
              + getSourceName(right) + " since the number of partitions don't "
              + "match. " + getSourceName(left) + " partitions = "
              + leftPartitions + "; " + getSourceName(right) + " partitions = "
              + rightPartitions + ". Please repartition either one so that the "
              + "number of partitions match.");
    }
  }

  private static String getSourceName(final DataSourceNode node) {
    return node.getDataSource().getName().name();
  }

  private static Column validateSchemaColumn(final ColumnRef column, final LogicalSchema schema) {
    return schema.findValueColumn(column)
        .orElseThrow(() -> new IllegalArgumentException(
            "Invalid join field, not found in schema: " + column));
  }

  private static class JoinerFactory {

    private final Map<
        Pair<DataSourceType, DataSourceType>,
        Supplier<Joiner>> joinerMap;

    JoinerFactory(
        final KsqlQueryBuilder builder,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      this.joinerMap = ImmutableMap.of(
          new Pair<>(DataSourceType.KSTREAM, DataSourceType.KSTREAM),
          () -> new StreamToStreamJoiner(builder, joinNode, contextStacker),
          new Pair<>(DataSourceType.KSTREAM, DataSourceType.KTABLE),
          () -> new StreamToTableJoiner(builder, joinNode, contextStacker),
          new Pair<>(DataSourceType.KTABLE, DataSourceType.KTABLE),
          () -> new TableToTableJoiner(builder, joinNode, contextStacker)
      );
    }

    Joiner getJoiner(final DataSourceType leftType,
        final DataSourceType rightType) {

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

    SchemaKStream<K> buildStream(
        final PlanNode node,
        final ColumnRef joinFieldName
    ) {
      return maybeRePartitionByKey(
          node.buildStream(builder),
          joinFieldName,
          contextStacker
      );
    }

    @SuppressWarnings("unchecked")
    SchemaKTable<K> buildTable(final PlanNode node) {
      final SchemaKStream<?> schemaKStream = node.buildStream(
          builder.withKsqlConfig(builder.getKsqlConfig()
              .cloneWithPropertyOverwrite(Collections.singletonMap(
                  ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")))
      );

      if (!(schemaKStream instanceof SchemaKTable)) {
        throw new RuntimeException("Expected to find a Table, found a stream instead.");
      }

      return (SchemaKTable<K>) schemaKStream;
    }

    @SuppressWarnings("unchecked")
    static <K> SchemaKStream<K> maybeRePartitionByKey(
        final SchemaKStream stream,
        final ColumnRef joinFieldName,
        final Stacker contextStacker
    ) {
      return stream.selectKey(new ColumnReferenceExp(joinFieldName), contextStacker);
    }

    static ValueFormat getFormatForSource(final DataSourceNode sourceNode) {
      return sourceNode.getDataSource()
          .getKsqlTopic()
          .getValueFormat();
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

    @Override
    public SchemaKStream<K> join() {
      if (!joinNode.withinExpression.isPresent()) {
        throw new KsqlException("Stream-Stream joins must have a WITHIN clause specified. None was "
            + "provided. To learn about how to specify a WITHIN clause with a "
            + "stream-stream join, please visit: https://docs.confluent"
            + ".io/current/ksql/docs/syntax-reference.html"
            + "#create-stream-as-select");
      }

      final SchemaKStream<K> leftStream = buildStream(
          joinNode.getLeft(), joinNode.leftJoinFieldName);

      final SchemaKStream<K> rightStream = buildStream(
          joinNode.getRight(), joinNode.rightJoinFieldName);

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(
              rightStream,
              joinNode.keyField,
              joinNode.withinExpression.get().joinWindow(),
              getFormatForSource(joinNode.left),
              getFormatForSource(joinNode.right),
              contextStacker
          );
        case OUTER:
          return leftStream.outerJoin(
              rightStream,
              joinNode.keyField,
              joinNode.withinExpression.get().joinWindow(),
              getFormatForSource(joinNode.left),
              getFormatForSource(joinNode.right),
              contextStacker
          );
        case INNER:
          return leftStream.join(
              rightStream,
              joinNode.keyField,
              joinNode.withinExpression.get().joinWindow(),
              getFormatForSource(joinNode.left),
              getFormatForSource(joinNode.right),
              contextStacker
          );
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

    @Override
    public SchemaKStream<K> join() {
      if (joinNode.withinExpression.isPresent()) {
        throw new KsqlException("A window definition was provided for a Stream-Table join. These "
            + "joins are not windowed. Please drop the window definition (ie."
            + " the WITHIN clause) and try to execute your join again.");
      }

      final SchemaKTable<K> rightTable = buildTable(joinNode.getRight());

      final SchemaKStream<K> leftStream = buildStream(
          joinNode.getLeft(), joinNode.leftJoinFieldName);

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(
              rightTable,
              joinNode.keyField,
              getFormatForSource(joinNode.left),
              contextStacker
          );

        case INNER:
          return leftStream.join(
              rightTable,
              joinNode.keyField,
              getFormatForSource(joinNode.left),
              contextStacker
          );
        case OUTER:
          throw new KsqlException("Full outer joins between streams and tables are not supported.");

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

    @Override
    public SchemaKTable<K> join() {
      if (joinNode.withinExpression.isPresent()) {
        throw new KsqlException("A window definition was provided for a Table-Table join. These "
            + "joins are not windowed. Please drop the window definition "
            + "(i.e. the WITHIN clause) and try to execute your Table-Table "
            + "join again.");
      }

      final SchemaKTable<K> leftTable = buildTable(joinNode.getLeft());
      final SchemaKTable<K> rightTable = buildTable(joinNode.getRight());

      switch (joinNode.joinType) {
        case LEFT:
          return leftTable.leftJoin(
              rightTable,
              joinNode.keyField,
              contextStacker);
        case INNER:
          return leftTable.join(
              rightTable,
              joinNode.keyField,
              contextStacker);
        case OUTER:
          return leftTable.outerJoin(
              rightTable,
              joinNode.keyField,
              contextStacker);
        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }
    }
  }

  private static DataSourceType calculateSinkType(
      final DataSourceNode left,
      final DataSourceNode right
  ) {
    final DataSourceType leftType = left.getDataSourceType();
    final DataSourceType rightType = right.getDataSourceType();
    return leftType == DataSourceType.KTABLE && rightType == DataSourceType.KTABLE
        ? DataSourceType.KTABLE
        : DataSourceType.KSTREAM;
  }

  private static LogicalSchema buildJoinSchema(
      final DataSourceNode left,
      final ColumnRef leftJoinFieldName,
      final DataSourceNode right,
      final ColumnRef rightJoinFieldName
  ) {
    final LogicalSchema leftSchema = selectKey(left, leftJoinFieldName);
    final LogicalSchema rightSchema = selectKey(right, rightJoinFieldName);

    return JoinParamsFactory.createSchema(leftSchema, rightSchema);
  }

  /**
   * Adjust the schema to take into account any change in key columns.
   *
   * @param source the source node
   * @param joinColumnRef the join column
   * @return the true source schema after any change of key columns.
   */
  private static LogicalSchema selectKey(
      final DataSourceNode source,
      final ColumnRef joinColumnRef
  ) {
    final LogicalSchema sourceSchema = source.getSchema();

    final Column joinCol = sourceSchema.findColumn(joinColumnRef)
        .orElseThrow(() -> new KsqlException("Unknown join column: " + joinColumnRef));

    if (sourceSchema.key().size() != 1) {
      throw new UnsupportedOperationException("Only single key columns supported");
    }

    if (joinCol.namespace() == Namespace.KEY) {
      // Join column is only key column, so no change of key columns required:
      return sourceSchema;
    }

    final Optional<Column> keyColumn = source
        .getKeyField()
        .resolve(sourceSchema);

    if (keyColumn.isPresent() && keyColumn.get().equals(joinCol)) {
      // Join column is KEY field, which is an alias for the only key column, so no change of key
      // columns required:
      return sourceSchema;
    }

    // Change of key columns required

    if (source.getDataSourceType() == DataSourceType.KTABLE) {
      // Tables do not support rekey:
      final String sourceName = source.getDataSource().getName().toString(FormatOptions.noEscape());

      if (!keyColumn.isPresent()) {
        throw new KsqlException(
            "Invalid join criteria: Source table (" + sourceName + ") has no key column "
                + "defined. Only 'ROWKEY' is supported in the join criteria for a TABLE."
        );
      }

      throw new KsqlException(
          "Invalid join criteria: Source table "
              + "(" + sourceName + ") key column "
              + "(" + keyColumn.get().ref().toString(FormatOptions.noEscape()) + ") "
              + "is not the column used in the join criteria ("
              + joinCol.ref().toString(FormatOptions.noEscape()) + "). "
              + "Only the table's key column or 'ROWKEY' is supported in the join criteria "
              + "for a TABLE."
      );
    }

    return LogicalSchema.builder()
        .keyColumn(source.getAlias(), SchemaUtil.ROWKEY_NAME, joinCol.type())
        .valueColumns(sourceSchema.value())
        .build();
  }
}
