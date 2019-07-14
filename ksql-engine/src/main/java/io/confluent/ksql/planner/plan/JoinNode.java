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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
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
  private final DataSourceNode left;
  private final DataSourceNode right;
  private final LogicalSchema schema;
  private final String leftJoinFieldName;
  private final String rightJoinFieldName;
  private final KeyField keyField;
  private final Optional<WithinExpression> withinExpression;

  public JoinNode(
      final PlanNodeId id,
      final JoinType joinType,
      final DataSourceNode left,
      final DataSourceNode right,
      final String leftJoinFieldName,
      final String rightJoinFieldName,
      final Optional<WithinExpression> withinExpression
  ) {
    super(id, calculateSinkType(left, right));
    this.joinType = joinType;
    this.left = Objects.requireNonNull(left, "left");
    this.right = Objects.requireNonNull(right, "right");
    this.leftJoinFieldName = Objects.requireNonNull(leftJoinFieldName, "leftJoinFieldName");
    this.rightJoinFieldName = Objects.requireNonNull(rightJoinFieldName, "rightJoinFieldName");
    this.withinExpression = Objects.requireNonNull(withinExpression, "withinExpression");

    final Field leftKeyField = validateFieldInSchema(leftJoinFieldName, left.getSchema());
    validateFieldInSchema(rightJoinFieldName, right.getSchema());

    this.keyField = KeyField.of(leftJoinFieldName, leftKeyField);

    this.schema = buildSchema(left, right);
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
        builder.buildNodeContext(getId()));

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
    return node.getDataSource().getName();
  }

  private static Field validateFieldInSchema(final String fieldName, final LogicalSchema schema) {
    return schema.findValueField(fieldName)
        .orElseThrow(() -> new IllegalArgumentException(
            "Invalid join field, not found in schema: " + fieldName));
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

    protected SchemaKStream<K> buildStream(
        final PlanNode node,
        final String joinFieldName
    ) {
      return maybeRePartitionByKey(
          node.buildStream(builder),
          joinFieldName,
          contextStacker);
    }

    @SuppressWarnings("unchecked")
    protected SchemaKTable<K> buildTable(
        final PlanNode node,
        final String joinFieldName,
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

      final Optional<Field> keyField = schemaKStream
          .getKeyField()
          .resolve(schemaKStream.getSchema(), builder.getKsqlConfig());

      final String rowKey = SchemaUtil.buildAliasedFieldName(tableName, SchemaUtil.ROWKEY_NAME);

      final boolean namesMatch = keyField
          .map(field -> SchemaUtil.matchFieldName(field, joinFieldName))
          .orElse(false);

      if (namesMatch || joinFieldName.equals(rowKey)) {
        return (SchemaKTable) schemaKStream;
      }

      if (!keyField.isPresent()) {
        throw new KsqlException(
            "Source table (" + tableName + ") has no key column defined. "
                + "Only 'ROWKEY' is supported in the join criteria."
        );
      }

      throw new KsqlException(
          "Source table (" + tableName + ") key column (" + keyField.get().name() + ") "
              + "is not the column used in the join criteria (" + joinFieldName + "). "
              + "Only the table's key column or 'ROWKEY' is supported in the join criteria."
      );
    }

    @SuppressWarnings("unchecked")
    static <K> SchemaKStream<K> maybeRePartitionByKey(
        final SchemaKStream stream,
        final String joinFieldName,
        final QueryContext.Stacker contextStacker
    ) {
      final LogicalSchema schema = stream.getSchema();

      schema.findValueField(joinFieldName)
          .orElseThrow(() ->
              new KsqlException("couldn't find key field: " + joinFieldName + " in schema"));

      return stream.selectKey(joinFieldName, true, contextStacker);
    }

    Serde<GenericRow> getSerDeForSource(
        final DataSourceNode sourceNode,
        final QueryContext.Stacker contextStacker
    ) {
      final DataSource<?> dataSource = sourceNode.getDataSource();

      final KsqlSerdeFactory valueSerdeFactory = dataSource
          .getKsqlTopic()
          .getValueSerdeFactory();

      final LogicalSchema logicalSchema = sourceNode.getSchema()
          .withoutAlias();

      return builder.buildGenericRowSerde(
          valueSerdeFactory,
          PhysicalSchema.from(
              logicalSchema,
              SerdeOption.none()
          ),
          contextStacker.getQueryContext()
      );
    }

    /**
     * The key field of the resultant joined stream.
     *
     * @param leftAlias the alias of the left source.
     * @param leftKeyField the key field of the left source.
     * @return the key field that should be used by the resultant joined stream.
     */
    static KeyField getJoinedKeyField(final String leftAlias, final KeyField leftKeyField) {
      final Optional<String> latest = Optional
          .of(leftKeyField.name().orElse(SchemaUtil.ROWKEY_NAME));

      return KeyField.of(latest, leftKeyField.legacy())
          .withAlias(leftAlias);
    }

    /**
     * The key field of the resultant joined stream for OUTER joins.
     *
     * <p>Note: for outer joins neither source's key field can be used as they may be null.
     *
     * @param leftAlias the alias of the left source.
     * @param leftKeyField the key field of the left source.
     * @return the key field that should be used by the resultant joined stream.
     */
    static KeyField getOuterJoinedKeyField(final String leftAlias, final KeyField leftKeyField) {
      return KeyField.none()
          .withLegacy(leftKeyField.legacy())
          .withAlias(leftAlias);
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
              joinNode.schema,
              getJoinedKeyField(joinNode.left.getAlias(), leftStream.getKeyField()),
              joinNode.withinExpression.get().joinWindow(),
              getSerDeForSource(joinNode.left, contextStacker.push(LEFT_SERDE_CONTEXT_NAME)),
              getSerDeForSource(joinNode.right, contextStacker.push(RIGHT_SERDE_CONTEXT_NAME)),
              contextStacker);
        case OUTER:
          return leftStream.outerJoin(
              rightStream,
              joinNode.schema,
              getOuterJoinedKeyField(joinNode.left.getAlias(), leftStream.getKeyField()),
              joinNode.withinExpression.get().joinWindow(),
              getSerDeForSource(joinNode.left, contextStacker.push(LEFT_SERDE_CONTEXT_NAME)),
              getSerDeForSource(joinNode.right, contextStacker.push(RIGHT_SERDE_CONTEXT_NAME)),
              contextStacker);
        case INNER:
          return leftStream.join(
              rightStream,
              joinNode.schema,
              getJoinedKeyField(joinNode.left.getAlias(), leftStream.getKeyField()),
              joinNode.withinExpression.get().joinWindow(),
              getSerDeForSource(joinNode.left, contextStacker.push(LEFT_SERDE_CONTEXT_NAME)),
              getSerDeForSource(joinNode.right, contextStacker.push(RIGHT_SERDE_CONTEXT_NAME)),
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

    @Override
    public SchemaKStream<K> join() {
      if (joinNode.withinExpression.isPresent()) {
        throw new KsqlException("A window definition was provided for a Stream-Table join. These "
            + "joins are not windowed. Please drop the window definition (ie."
            + " the WITHIN clause) and try to execute your join again.");
      }

      final SchemaKTable<K> rightTable = buildTable(
          joinNode.getRight(), joinNode.rightJoinFieldName, joinNode.right.getAlias());

      final SchemaKStream<K> leftStream = buildStream(
          joinNode.getLeft(), joinNode.leftJoinFieldName);

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(
              rightTable,
              joinNode.schema,
              getJoinedKeyField(joinNode.left.getAlias(), leftStream.getKeyField()),
              getSerDeForSource(joinNode.left, contextStacker.push(LEFT_SERDE_CONTEXT_NAME)),
              contextStacker);

        case INNER:
          return leftStream.join(
              rightTable,
              joinNode.schema,
              getJoinedKeyField(joinNode.left.getAlias(), leftStream.getKeyField()),
              getSerDeForSource(joinNode.left, contextStacker.push(LEFT_SERDE_CONTEXT_NAME)),
              contextStacker);
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

      final SchemaKTable<K> leftTable = buildTable(
          joinNode.getLeft(), joinNode.leftJoinFieldName, joinNode.left.getAlias());
      final SchemaKTable<K> rightTable = buildTable(
          joinNode.getRight(), joinNode.rightJoinFieldName, joinNode.right.getAlias());

      switch (joinNode.joinType) {
        case LEFT:
          return leftTable.leftJoin(
              rightTable,
              joinNode.schema,
              getJoinedKeyField(joinNode.left.getAlias(), leftTable.getKeyField()),
              contextStacker);
        case INNER:
          return leftTable.join(
              rightTable,
              joinNode.schema,
              getJoinedKeyField(joinNode.left.getAlias(), leftTable.getKeyField()),
              contextStacker);
        case OUTER:
          return leftTable.outerJoin(
              rightTable,
              joinNode.schema,
              getOuterJoinedKeyField(joinNode.left.getAlias(), leftTable.getKeyField()),
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

  private static LogicalSchema buildSchema(
      final PlanNode left,
      final PlanNode right
  ) {

    final LogicalSchema leftSchema = left.getSchema();
    final LogicalSchema rightSchema = right.getSchema();

    final SchemaBuilder valueSchema = SchemaBuilder.struct();

    for (final Field field : leftSchema.valueSchema().fields()) {
      valueSchema.field(field.name(), field.schema());
    }

    for (final Field field : rightSchema.valueSchema().fields()) {
      valueSchema.field(field.name(), field.schema());
    }

    // Hard-wire for now, until we support custom type/name of key fields:
    final Schema keySchema = SchemaBuilder.struct()
        .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    return LogicalSchema.of(keySchema, valueSchema.build());
  }
}
