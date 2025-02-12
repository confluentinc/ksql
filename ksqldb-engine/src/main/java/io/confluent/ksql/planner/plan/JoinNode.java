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

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.streams.ForeignKeyJoinParamsFactory;
import io.confluent.ksql.execution.streams.JoinParamsFactory;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.planner.RequiredColumns;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.none.NoneFormat;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class JoinNode extends PlanNode implements JoiningNode {

  public enum JoinType {
    INNER, LEFT, RIGHT, OUTER;

    @Override
    public String toString() {
      switch (this) {
        case INNER:
          return "[INNER] JOIN";
        case LEFT:
          return "LEFT [OUTER] JOIN";
        case RIGHT:
          return "RIGHT [OUTER] JOIN";
        case OUTER:
          return "[FULL] OUTER JOIN";
        default:
          throw new IllegalStateException();
      }
    }
  }

  private final JoinType joinType;
  private final JoinKey joinKey;
  private final boolean finalJoin;
  private final PlanNode left;
  private final PlanNode right;
  private final JoiningNode leftJoining;
  private final JoiningNode rightJoining;
  private final Optional<WithinExpression> withinExpression;
  private final LogicalSchema schema;
  private final String defaultKeyFormat;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public JoinNode(
      final PlanNodeId id,
      final JoinType joinType,
      final JoinKey joinKey,
      final boolean finalJoin,
      final PlanNode left,
      final PlanNode right,
      final Optional<WithinExpression> withinExpression,
      final String defaultKeyFormat
  ) {
    super(
        id,
        calculateSinkType(left, right),
        Optional.empty()
    );

    this.joinType = requireNonNull(joinType, "joinType");
    this.joinKey = requireNonNull(joinKey, "joinKey");
    this.schema = joinKey.isForeignKey()
        ? ForeignKeyJoinParamsFactory.createSchema(left.getSchema(), right.getSchema())
        : buildJoinSchema(joinKey, left, right);
    this.finalJoin = finalJoin;
    this.left = requireNonNull(left, "left");
    this.leftJoining = (JoiningNode) left;
    this.right = requireNonNull(right, "right");
    this.rightJoining = (JoiningNode) right;
    this.withinExpression = requireNonNull(withinExpression, "withinExpression");
    this.defaultKeyFormat = requireNonNull(defaultKeyFormat, "defaultKeyFormat");
  }

  /**
   * Determines the key format of the join.
   *
   * <p>For now, the left key format is the preferred join key format unless the
   * right source is not already being repartitioned and the left source is.
   *
   * @see <a href="https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-33-key-format.md">KLIP-33</a>
   */
  public void resolveKeyFormats() {
    final KeyFormat joinKeyFormat = getPreferredKeyFormat()
        .orElseGet(this::getDefaultSourceKeyFormat);

    setKeyFormat(joinKeyFormat);
  }

  @Override
  public Optional<KeyFormat> getPreferredKeyFormat() {
    final Optional<KeyFormat> leftPreferred = leftJoining.getPreferredKeyFormat();
    return leftPreferred.isPresent()
        ? leftPreferred
        : rightJoining.getPreferredKeyFormat();
  }

  @Override
  public void setKeyFormat(final KeyFormat format) {
    leftJoining.setKeyFormat(format);
    rightJoining.setKeyFormat(format);
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public List<PlanNode> getSources() {
    return Arrays.asList(left, right);
  }

  public PlanNode getLeft() {
    return left;
  }

  public PlanNode getRight() {
    return right;
  }

  @Override
  public SchemaKStream<?> buildStream(final PlanBuildContext buildContext) {

    if (!joinKey.isForeignKey()) {
      ensureMatchingPartitionCounts(buildContext.getServiceContext().getTopicClient());
    }

    final JoinerFactory joinerFactory = new JoinerFactory(
        buildContext,
        this,
        buildContext.buildNodeContext(getId().toString()));

    return joinerFactory.getJoiner(left.getNodeOutputType(), right.getNodeOutputType()).join();
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return right.getPartitions(kafkaTopicClient);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Override
  public Stream<ColumnName> resolveSelectStar(
      final Optional<SourceName> sourceName
  ) {
    final Stream<ColumnName> names = Stream.of(left, right)
        .flatMap(JoinNode::getPreJoinProjectDataSources)
        .filter(s -> !sourceName.isPresent() || sourceName.equals(s.getSourceName()))
        .flatMap(s -> s.resolveSelectStar(sourceName));

    if (sourceName.isPresent() || !joinKey.isSynthetic() || !finalJoin) {
      return names;
    }

    // if we use a synthetic key, we know there's only a single key element
    final Column syntheticKey = getOnlyElement(getSchema().key());

    return Streams.concat(Stream.of(syntheticKey.name()), names);
  }

  @Override
  void validateKeyPresent(final SourceName sinkName, final Projection projection) {

    if (joinKey.isForeignKey()) {
      final DataSourceNode leftInputTable = getLeftmostSourceNode();
      final SourceName leftInputTableName = leftInputTable.getAlias();
      final List<Column> leftInputTableKeys = leftInputTable.getDataSource().getSchema().key();
      final List<Column> missingKeys =
          leftInputTableKeys.stream().filter(
              k -> !projection.containsExpression(
                    new QualifiedColumnReferenceExp(leftInputTableName, k.name()))
                  && !projection.containsExpression(new UnqualifiedColumnReferenceExp(
                      ColumnNames.generatedJoinColumnAlias(leftInputTableName, k.name())
                  ))
          ).collect(Collectors.toList());

      if (!missingKeys.isEmpty()) {
        throwMissingKeyColumnForFkJoinException(missingKeys, leftInputTableName);
      }
    } else {
      final boolean atLeastOneKey = joinKey.getAllViableKeys(schema).stream()
          .anyMatch(projection::containsExpression);

      if (!atLeastOneKey) {
        final boolean synthetic = joinKey.isSynthetic();
        final List<? extends Expression> viable = joinKey.getOriginalViableKeys(schema);

        throwKeysNotIncludedError(sinkName, "join expression", viable, false, synthetic);
      }
    }
  }

  private static void throwMissingKeyColumnForFkJoinException(
      final List<Column> missingKeys,
      final SourceName leftInputTableName
  ) {
    final String columnString = missingKeys.size() == 1 ? "column" : "columns";
    final String plainInputTableName = leftInputTableName.text();
    // column names may be represented as `TABLENAME_COLUMNNAME`
    final int tablePrefixLength = plainInputTableName.length() + 1;
    throw new KsqlException(String.format(
        "Primary key %s missing in projection."
            + " For foreign-key table-table joins, the projection must include"
            + " all primary key columns from the left input table (%s)."
            + " Missing %s: %s.",
        columnString,
        leftInputTableName,
        columnString,
        missingKeys.stream()
            .map(c -> c.name().text())
            .map(name -> name.startsWith(plainInputTableName)
                ? name.substring(tablePrefixLength)
                : name)
            .map(name -> String.format("`%s`", name))
            .collect(Collectors.joining(", "))
    ));
  }

  @Override
  protected Set<ColumnReferenceExp> validateColumns(
      final RequiredColumns requiredColumns
  ) {
    final boolean noSyntheticKey = !finalJoin || !joinKey.isSynthetic();

    final RequiredColumns updated = noSyntheticKey
        ? requiredColumns
        : requiredColumns.asBuilder()
          .remove(new UnqualifiedColumnReferenceExp(getOnlyElement(schema.key()).name()))
          .build();

    final Set<ColumnReferenceExp> leftUnknown = left.validateColumns(updated);
    final Set<ColumnReferenceExp> rightUnknown = right.validateColumns(updated);

    return Sets.intersection(leftUnknown, rightUnknown);
  }

  private ColumnName getKeyColumnName() {
    if (getSchema().key().size() > 1) {
      throw new KsqlException("JOINs are not supported with multiple key columns: "
          + getSchema().key());
    }

    return getOnlyElement(getSchema().key()).name();
  }

  private void ensureMatchingPartitionCounts(final KafkaTopicClient kafkaTopicClient) {
    final int leftPartitions = left.getPartitions(kafkaTopicClient);
    final int rightPartitions = right.getPartitions(kafkaTopicClient);
    if (leftPartitions == rightPartitions) {
      return;
    }

    final SourceName leftSource = getSourceName(left);
    final SourceName rightSource = getSourceName(right);

    throw new KsqlException(
        "Can't join " + leftSource + " with "
            + rightSource + " since the number of partitions don't "
            + "match. " + leftSource + " partitions = "
            + leftPartitions + "; " + rightSource + " partitions = "
            + rightPartitions + ". Please repartition either one so that the "
            + "number of partitions match.");
  }

  private KeyFormat getDefaultSourceKeyFormat() {
    return Stream.of(left, right)
        .flatMap(PlanNode::getSourceNodes)
        .map(DataSourceNode::getDataSource)
        .map(DataSource::getKsqlTopic)
        .map(KsqlTopic::getKeyFormat)
        .filter(format -> !format.getFormatInfo().getFormat().equals(NoneFormat.NAME))
        .findFirst()
        // if none exist, assume non-Windowed since that would mean that both sources
        // were of NONE format, which doesn't support windowed operations
        .orElse(KeyFormat.nonWindowed(FormatInfo.of(defaultKeyFormat), SerdeFeatures.of()));
  }

  private static SourceName getSourceName(final PlanNode node) {
    return node.getLeftmostSourceNode().getAlias();
  }

  private static class JoinerFactory {

    private final Map<
        Pair<DataSourceType, DataSourceType>,
        Supplier<Joiner<?>>> joinerMap;

    JoinerFactory(
        final PlanBuildContext buildContext,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      this.joinerMap = ImmutableMap.of(
          new Pair<>(DataSourceType.KSTREAM, DataSourceType.KSTREAM),
          () -> new StreamToStreamJoiner<>(buildContext, joinNode, contextStacker),
          new Pair<>(DataSourceType.KSTREAM, DataSourceType.KTABLE),
          () -> new StreamToTableJoiner<>(buildContext, joinNode, contextStacker),
          new Pair<>(DataSourceType.KTABLE, DataSourceType.KTABLE),
          () -> new TableToTableJoiner<>(buildContext, joinNode, contextStacker)
      );
    }

    Joiner<?> getJoiner(final DataSourceType leftType, final DataSourceType rightType) {
      final Supplier<Joiner<?>> joinerSupplier = joinerMap.get(new Pair<>(leftType, rightType));
      if (joinerSupplier == null) {
        throw new IllegalStateException("Invalid joiner supplier requested: left type: "
            + leftType + ", right type: " + rightType);
      }
      return joinerSupplier.get();
    }
  }

  private abstract static class Joiner<K> {

    final PlanBuildContext buildContext;
    final JoinNode joinNode;
    final QueryContext.Stacker contextStacker;

    Joiner(
        final PlanBuildContext buildContext,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      this.buildContext = requireNonNull(buildContext, "buildContext");
      this.joinNode = requireNonNull(joinNode, "joinNode");
      this.contextStacker = requireNonNull(contextStacker, "contextStacker");
    }

    public abstract SchemaKStream<K> join();

    @SuppressWarnings("unchecked")
    SchemaKStream<K> buildStream(final PlanNode node) {
      return (SchemaKStream<K>) node.buildStream(buildContext);
    }

    @SuppressWarnings("unchecked")
    SchemaKTable<K> buildTable(final PlanNode node) {
      final SchemaKStream<?> schemaKStream = node.buildStream(
          buildContext.withKsqlConfig(buildContext.getKsqlConfig()
              .cloneWithPropertyOverwrite(Collections.singletonMap(
                  ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")))
      );

      if (!(schemaKStream instanceof SchemaKTable)) {
        throw new RuntimeException("Expected to find a Table, found a stream instead.");
      }

      return ((SchemaKTable<K>) schemaKStream);
    }
  }

  private static final class StreamToStreamJoiner<K> extends Joiner<K> {
    private StreamToStreamJoiner(
        final PlanBuildContext buildContext,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      super(buildContext, joinNode, contextStacker);
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

      final SchemaKStream<K> leftStream = buildStream(joinNode.getLeft());
      final SchemaKStream<K> rightStream = buildStream(joinNode.getRight());

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(
              rightStream,
              joinNode.getKeyColumnName(),
              joinNode.withinExpression.get(),
              JoiningNode.getValueFormatForSource(joinNode.left).getFormatInfo(),
              JoiningNode.getValueFormatForSource(joinNode.right).getFormatInfo(),
              contextStacker
          );
        case RIGHT:
          return leftStream.rightJoin(
              rightStream,
              joinNode.getKeyColumnName(),
              joinNode.withinExpression.get(),
              JoiningNode.getValueFormatForSource(joinNode.left).getFormatInfo(),
              JoiningNode.getValueFormatForSource(joinNode.right).getFormatInfo(),
              contextStacker
          );
        case OUTER:
          return leftStream.outerJoin(
              rightStream,
              joinNode.getKeyColumnName(),
              joinNode.withinExpression.get(),
              JoiningNode.getValueFormatForSource(joinNode.left).getFormatInfo(),
              JoiningNode.getValueFormatForSource(joinNode.right).getFormatInfo(),
              contextStacker
          );
        case INNER:
          return leftStream.innerJoin(
              rightStream,
              joinNode.getKeyColumnName(),
              joinNode.withinExpression.get(),
              JoiningNode.getValueFormatForSource(joinNode.left).getFormatInfo(),
              JoiningNode.getValueFormatForSource(joinNode.right).getFormatInfo(),
              contextStacker
          );
        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }
    }
  }

  private static final class StreamToTableJoiner<K> extends Joiner<K> {

    private StreamToTableJoiner(
        final PlanBuildContext buildContext,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      super(buildContext, joinNode, contextStacker);
    }

    @Override
    public SchemaKStream<K> join() {
      if (joinNode.withinExpression.isPresent()) {
        throw new KsqlException("A window definition was provided for a Stream-Table join. These "
            + "joins are not windowed. Please drop the window definition (ie."
            + " the WITHIN clause) and try to execute your join again.");
      }

      final SchemaKTable<K> rightTable = buildTable(joinNode.getRight());
      final SchemaKStream<K> leftStream = buildStream(joinNode.getLeft());

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(
              rightTable,
              joinNode.getKeyColumnName(),
              JoiningNode.getValueFormatForSource(joinNode.left).getFormatInfo(),
              contextStacker
          );

        case INNER:
          return leftStream.innerJoin(
              rightTable,
              joinNode.getKeyColumnName(),
              JoiningNode.getValueFormatForSource(joinNode.left).getFormatInfo(),
              contextStacker
          );

        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }
    }
  }

  private static final class TableToTableJoiner<K> extends Joiner<K> {

    TableToTableJoiner(
        final PlanBuildContext buildContext,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      super(buildContext, joinNode, contextStacker);
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

      final JoinKey joinKey = joinNode.joinKey;

      final FormatInfo valueFormatInfo = JoiningNode
          .getValueFormatForSource(joinNode.getLeft()).getFormatInfo();

      switch (joinNode.joinType) {
        case LEFT:
          if (joinKey.isForeignKey()) {
            return leftTable.foreignKeyLeftJoin(
                rightTable,
                ((ForeignJoinKey) joinKey).getForeignKeyColumn(),
                ((ForeignJoinKey) joinKey).getForeignKeyExpression(),
                contextStacker,
                valueFormatInfo
            );
          } else {
            return leftTable.leftJoin(
                rightTable,
                joinNode.getKeyColumnName(),
                contextStacker
            );
          }
        case RIGHT:
          if (joinKey.isForeignKey()) {
            throw new KsqlException("RIGHT OUTER JOIN on a foreign key is not supported");
          } else {
            return leftTable.rightJoin(
                rightTable,
                joinNode.getKeyColumnName(),
                contextStacker
            );
          }
        case INNER:
          if (joinKey.isForeignKey()) {
            return leftTable.foreignKeyInnerJoin(
                rightTable,
                ((ForeignJoinKey) joinKey).getForeignKeyColumn(),
                ((ForeignJoinKey) joinKey).getForeignKeyExpression(),
                contextStacker,
                valueFormatInfo
            );
          } else {
            return leftTable.innerJoin(
                rightTable,
                joinNode.getKeyColumnName(),
                contextStacker
            );
          }
        case OUTER:
          if (joinKey.isForeignKey()) {
            throw new IllegalStateException("Outer-join not supported by FK-joins.");
          }
          return leftTable.outerJoin(
              rightTable,
              joinNode.getKeyColumnName(),
              contextStacker);
        default:
          throw new KsqlException("Invalid join type encountered: " + joinNode.joinType);
      }
    }
  }

  private static DataSourceType calculateSinkType(
      final PlanNode left,
      final PlanNode right
  ) {
    final DataSourceType leftType = left.getNodeOutputType();
    final DataSourceType rightType = right.getNodeOutputType();
    return leftType == DataSourceType.KTABLE && rightType == DataSourceType.KTABLE
        ? DataSourceType.KTABLE
        : DataSourceType.KSTREAM;
  }

  private static LogicalSchema buildJoinSchema(
      final JoinKey joinKey,
      final PlanNode left,
      final PlanNode right
  ) {
    final ColumnName keyName = joinKey.resolveKeyName(left, right);
    return JoinParamsFactory.createSchema(keyName, left.getSchema(), right.getSchema());
  }

  /**
   * If there is a {@code JoinNode} upstream of the given node, this method
   * returns that {@code JoinNode}. Else, empty.
   */
  private static Optional<JoinNode> findUpstreamJoin(final PlanNode node) {
    PlanNode current = node;
    while (!(current instanceof JoinNode) && (current instanceof JoiningNode)) {
      if (current.getSources().size() != 1) {
        throw new IllegalStateException("Only JoinNode can have multiple sources.");
      }
      current = current.getSources().get(0);
    }
    if (current instanceof JoinNode) {
      return Optional.of((JoinNode) current);
    } else {
      return Optional.empty();
    }
  }

  /**
   * Analogous to {@link PlanNode#getSourceNodes()} except instead of returning
   * {@code DataSourceNode}s, this method returns the {@code PreJoinProjectNode}s
   * attached to those {@code DataSourceNode}s. This is achieved by traversing
   * the join tree and returning the plan nodes immediately preceding any
   * {@code JoinNode}s until no more {@code JoinNode}s are present in any particular
   * branch of the tree.
   */
  private static Stream<PlanNode> getPreJoinProjectDataSources(final PlanNode node) {
    final Optional<JoinNode> upstreamJoin = findUpstreamJoin(node);
    return upstreamJoin.map(n ->
        n.getSources().stream()
            .flatMap(JoinNode::getPreJoinProjectDataSources)
    ).orElse(Stream.of(node));
  }

  @Immutable
  public interface JoinKey {

    static JoinKey sourceColumn(
        final ColumnName keyColumn,
        final Collection<QualifiedColumnReferenceExp> viableKeyColumns
    ) {
      return SourceJoinKey.of(keyColumn, viableKeyColumns);
    }

    static JoinKey syntheticColumn() {
      return SyntheticJoinKey.of();
    }

    static JoinKey foreignKey(
        final Expression foreignKeyExpression,
        final Collection<QualifiedColumnReferenceExp> viableKeyColumns
    ) {
      return ForeignJoinKey.of(foreignKeyExpression, viableKeyColumns);
    }

    /**
     * @return {@code true} if the join key is synthetic.
     */
    boolean isSynthetic();

    /**
     * @return {@code true} if the join key is a foreign-key join.
     */
    boolean isForeignKey();

    /**
     * @param schema the join schema.
     * @return the list of all viable key expressions.
     */
    List<? extends Expression> getAllViableKeys(LogicalSchema schema);

    /**
     * @param schema the join schema.
     * @return the list of viable key expressions, without any rewriting applied.
     */
    List<? extends Expression> getOriginalViableKeys(LogicalSchema schema);

    /**
     * @return Given the left and right schemas, the name of the join key.
     */
    ColumnName resolveKeyName(PlanNode left, PlanNode right);

    /**
     * Rewrite the join key with the supplied plugin.
     *
     * @param plugin the plugin to use.
     * @return the rewritten join key.
     */
    JoinKey rewriteWith(BiFunction<Expression, Context<Void>, Optional<Expression>> plugin);
  }

  private static final class SourceJoinKey implements JoinKey {

    private final ColumnName keyColumn;
    private final ImmutableList<QualifiedColumnReferenceExp> originalViableKeyColumns;
    private final ImmutableList<? extends ColumnReferenceExp> viableKeyColumns;

    static JoinKey of(
        final ColumnName keyColumn,
        final Collection<QualifiedColumnReferenceExp> viableKeyColumns
    ) {
      return new SourceJoinKey(keyColumn, viableKeyColumns, viableKeyColumns);
    }

    private SourceJoinKey(
        final ColumnName keyColumn,
        final Collection<QualifiedColumnReferenceExp> originalViableKeyColumns,
        final Collection<? extends ColumnReferenceExp> viableKeyColumns
    ) {
      this.keyColumn = requireNonNull(keyColumn, "keyColumn");
      this.originalViableKeyColumns = ImmutableList
          .copyOf(requireNonNull(originalViableKeyColumns, "originalViableKeyColumns"));
      this.viableKeyColumns = ImmutableList
          .copyOf(requireNonNull(viableKeyColumns, "viableKeyColumns"));
    }

    @Override
    public boolean isSynthetic() {
      return false;
    }

    @Override
    public boolean isForeignKey() {
      return false;
    }

    @Override
    public List<? extends Expression> getAllViableKeys(final LogicalSchema schema) {
      return ImmutableList.<Expression>builder()
          .addAll(viableKeyColumns)
          .addAll(originalViableKeyColumns)
          .build();
    }

    @Override
    public List<? extends Expression> getOriginalViableKeys(final LogicalSchema schema) {
      return originalViableKeyColumns;
    }

    @Override
    public ColumnName resolveKeyName(final PlanNode left, final PlanNode right) {
      return keyColumn;
    }

    @Override
    public JoinKey rewriteWith(
        final BiFunction<Expression, Context<Void>, Optional<Expression>> plugin
    ) {
      final List<? extends ColumnReferenceExp> rewrittenViable = viableKeyColumns.stream()
          .map(e -> ExpressionTreeRewriter.rewriteWith(plugin, e))
          .collect(Collectors.toList());

      return new SourceJoinKey(keyColumn, originalViableKeyColumns, rewrittenViable);
    }
  }

  private static final class SyntheticJoinKey implements JoinKey {

    static JoinKey of() {
      return new SyntheticJoinKey();
    }

    private SyntheticJoinKey() {
    }

    @Override
    public boolean isSynthetic() {
      return true;
    }

    @Override
    public boolean isForeignKey() {
      return false;
    }

    @Override
    public List<? extends Expression> getAllViableKeys(final LogicalSchema schema) {
      return getOriginalViableKeys(schema);
    }

    @Override
    public List<? extends Expression> getOriginalViableKeys(final LogicalSchema schema) {
      return ImmutableList.of(
          new UnqualifiedColumnReferenceExp(getOnlyElement(schema.key()).name())
      );
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public ColumnName resolveKeyName(final PlanNode left, final PlanNode right) {
      // Need to collect source schemas from both sides to ensure a unique synthetic key column:
      // Only source schemas need be included as any synthetic keys introduced by internal
      // re-partitions or earlier joins in multi-way joins are only implementation details and
      // should not affect the naming of the synthetic key column.

      final Stream<LogicalSchema> schemas = Streams
          .concat(left.getSourceNodes(), right.getSourceNodes())
          .map(DataSourceNode::getDataSource)
          .map(DataSource::getSchema);

      return ColumnNames.generateSyntheticJoinKey(schemas);
    }

    @Override
    public JoinKey rewriteWith(
        final BiFunction<Expression, Context<Void>, Optional<Expression>> plugin
    ) {
      return this;
    }
  }

  private static final class ForeignJoinKey implements JoinKey {
    private final Optional<ColumnName> foreignKeyColumn;
    private final Optional<Expression> foreignKeyExpression;
    private final ImmutableList<? extends ColumnReferenceExp> leftSourceKeyColumns;

    static JoinKey of(final Expression foreignKeyExpression,
                      final Collection<QualifiedColumnReferenceExp> leftSourceKeyColumns) {
      return new ForeignJoinKey(
          Optional.empty(),
          Optional.of(foreignKeyExpression),
          leftSourceKeyColumns
      );
    }

    private ForeignJoinKey(final Optional<ColumnName> foreignKeyColumn,
                           final Optional<Expression> foreignKeyExpression,
                           final Collection<? extends ColumnReferenceExp> viableKeyColumns) {
      this.foreignKeyColumn = requireNonNull(foreignKeyColumn, "foreignKeyColumn");
      this.foreignKeyExpression =
          requireNonNull(foreignKeyExpression, "foreignKeyExpression");
      this.leftSourceKeyColumns = ImmutableList
          .copyOf(requireNonNull(viableKeyColumns, "viableKeyColumns"));
    }

    @Override
    public boolean isSynthetic() {
      return false;
    }

    @Override
    public boolean isForeignKey() {
      return true;
    }

    @Override
    public List<? extends Expression> getOriginalViableKeys(final LogicalSchema schema) {
      throw new UnsupportedOperationException("Should not be called with foreign key joins.");
    }

    @Override
    public List<? extends Expression> getAllViableKeys(final LogicalSchema schema) {
      throw new UnsupportedOperationException("Should not be called with foreign key joins.");
    }

    @Override
    public ColumnName resolveKeyName(final PlanNode left, final PlanNode right) {
      throw new UnsupportedOperationException("Should not be called with foreign key joins.");
    }

    @Override
    public JoinKey rewriteWith(
        final BiFunction<Expression, Context<Void>, Optional<Expression>> plugin) {
      final List<? extends ColumnReferenceExp> rewrittenViable = leftSourceKeyColumns.stream()
          .map(e -> ExpressionTreeRewriter.rewriteWith(plugin, e))
          .collect(Collectors.toList());

      return new ForeignJoinKey(Optional.empty(), foreignKeyExpression, rewrittenViable);
    }

    public Optional<ColumnName> getForeignKeyColumn() {
      return foreignKeyColumn;
    }

    public Optional<Expression> getForeignKeyExpression() {
      return foreignKeyExpression;
    }
  }
}
