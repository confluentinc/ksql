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

import static io.confluent.ksql.util.GrammaticalJoiner.or;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.streams.JoinParamsFactory;
import io.confluent.ksql.function.udf.JoinKeyUdf;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.planner.RequiredColumns;
import io.confluent.ksql.planner.RequiredColumns.Builder;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.ValueFormat;
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

public class JoinNode extends PlanNode {

  public enum JoinType {
    INNER, LEFT, OUTER
  }

  private final JoinType joinType;
  private final JoinKey joinKey;
  private final boolean finalJoin;
  private final PlanNode left;
  private final PlanNode right;
  private final Optional<WithinExpression> withinExpression;
  private final LogicalSchema schema;

  public JoinNode(
      final PlanNodeId id,
      final JoinType joinType,
      final JoinKey joinKey,
      final boolean finalJoin,
      final PlanNode left,
      final PlanNode right,
      final Optional<WithinExpression> withinExpression
  ) {
    super(
        id,
        calculateSinkType(left, right),
        Optional.empty()
    );

    this.schema = buildJoinSchema(joinKey, left, right);
    this.joinType = requireNonNull(joinType, "joinType");
    this.joinKey = requireNonNull(joinKey, "joinKey");
    this.finalJoin = finalJoin;
    this.left = requireNonNull(left, "left");
    this.right = requireNonNull(right, "right");
    this.withinExpression = requireNonNull(withinExpression, "withinExpression");
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
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {

    ensureMatchingPartitionCounts(builder.getServiceContext().getTopicClient());

    final JoinerFactory joinerFactory = new JoinerFactory(
        builder,
        this,
        builder.buildNodeContext(getId().toString()));

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
        .flatMap(s -> s instanceof JoinNode ? s.getSources().stream() : Stream.of(s))
        .filter(s -> !sourceName.isPresent() || sourceName.equals(s.getSourceName()))
        .flatMap(s -> s.resolveSelectStar(sourceName));

    if (sourceName.isPresent() || !joinKey.isSynthetic() || !finalJoin) {
      return names;
    }

    final Column syntheticKey = Iterables.getOnlyElement(getSchema().key());

    return Streams.concat(Stream.of(syntheticKey.name()), names);
  }

  public Expression resolveSelect(final int idx, final Expression expression) {
    return joinKey.resolveSelect(expression, getSchema());
  }

  @Override
  void validateKeyPresent(final SourceName sinkName, final Projection projection) {

    final boolean atLeastOneKey = joinKey.getAllViableKeys(schema).stream()
        .anyMatch(projection::containsExpression);

    if (!atLeastOneKey) {
      final List<? extends Expression> originalKeys = joinKey.getOriginalViableKeys();

      final Optional<Expression> syntheticKey = joinKey.isSynthetic()
          ? Optional.of(originalKeys.get(0))
          : Optional.empty();

      final String additional = syntheticKey
          .map(e -> System.lineSeparator()
              + e + " was added as a synthetic key column because the join criteria did "
              + "not match any source column. "
              + "This expression must be included in the projection and may be aliased. ")
          .orElse("");

      throwKeysNotIncludedError(sinkName, "join expression", originalKeys, or(), additional);
    }
  }

  @Override
  protected Set<ColumnReferenceExp> validateColumns(
      final RequiredColumns requiredColumns
  ) {
    final Builder builder = requiredColumns.asBuilder();

    if (finalJoin && joinKey.isSynthetic()) {
      builder.remove(
          new UnqualifiedColumnReferenceExp(Iterables.getOnlyElement(schema.key()).name())
      );
    }

    final RequiredColumns updated = builder.build();

    final Set<ColumnReferenceExp> leftUnknown = left.validateColumns(updated);
    final Set<ColumnReferenceExp> rightUnknown = right.validateColumns(updated);

    return Sets.intersection(leftUnknown, rightUnknown);
  }

  private ColumnName getKeyColumnName() {
    return Iterables.getOnlyElement(getSchema().key()).name();
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

  private static String getSourceName(final PlanNode node) {
    return node.getTheSourceNode().getAlias().text();
  }

  private static class JoinerFactory {

    private final Map<
        Pair<DataSourceType, DataSourceType>,
        Supplier<Joiner<?>>> joinerMap;

    JoinerFactory(
        final KsqlQueryBuilder builder,
        final JoinNode joinNode,
        final QueryContext.Stacker contextStacker
    ) {
      this.joinerMap = ImmutableMap.of(
          new Pair<>(DataSourceType.KSTREAM, DataSourceType.KSTREAM),
          () -> new StreamToStreamJoiner<>(builder, joinNode, contextStacker),
          new Pair<>(DataSourceType.KSTREAM, DataSourceType.KTABLE),
          () -> new StreamToTableJoiner<>(builder, joinNode, contextStacker),
          new Pair<>(DataSourceType.KTABLE, DataSourceType.KTABLE),
          () -> new TableToTableJoiner<>(builder, joinNode, contextStacker)
      );
    }

    Joiner<?> getJoiner(final DataSourceType leftType,
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
      this.builder = requireNonNull(builder, "builder");
      this.joinNode = requireNonNull(joinNode, "joinNode");
      this.contextStacker = requireNonNull(contextStacker, "contextStacker");
    }

    public abstract SchemaKStream<K> join();

    @SuppressWarnings("unchecked")
    SchemaKStream<K> buildStream(final PlanNode node) {
      return (SchemaKStream<K>) node.buildStream(builder);
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

      return ((SchemaKTable<K>) schemaKStream);
    }

    static ValueFormat getFormatForSource(final PlanNode sourceNode) {
      return sourceNode.getTheSourceNode()
          .getDataSource()
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
          joinNode.getLeft());

      final SchemaKStream<K> rightStream = buildStream(
          joinNode.getRight());

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(
              rightStream,
              joinNode.getKeyColumnName(),
              joinNode.withinExpression.get().joinWindow(),
              getFormatForSource(joinNode.left),
              getFormatForSource(joinNode.right),
              contextStacker
          );
        case OUTER:
          return leftStream.outerJoin(
              rightStream,
              joinNode.getKeyColumnName(),
              joinNode.withinExpression.get().joinWindow(),
              getFormatForSource(joinNode.left),
              getFormatForSource(joinNode.right),
              contextStacker
          );
        case INNER:
          return leftStream.join(
              rightStream,
              joinNode.getKeyColumnName(),
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
          joinNode.getLeft());

      switch (joinNode.joinType) {
        case LEFT:
          return leftStream.leftJoin(
              rightTable,
              joinNode.getKeyColumnName(),
              getFormatForSource(joinNode.left),
              contextStacker
          );

        case INNER:
          return leftStream.join(
              rightTable,
              joinNode.getKeyColumnName(),
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
              joinNode.getKeyColumnName(),
              contextStacker);
        case INNER:
          return leftTable.join(
              rightTable,
              joinNode.getKeyColumnName(),
              contextStacker);
        case OUTER:
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
    final ColumnName keyName = joinKey.resolveKeyName(left.getSchema(), right.getSchema());
    return JoinParamsFactory.createSchema(keyName, left.getSchema(), right.getSchema());
  }

  @Immutable
  public interface JoinKey {

    static JoinKey sourceColumn(
        final ColumnName keyColumn,
        final Collection<QualifiedColumnReferenceExp> viableKeyColumns
    ) {
      return SourceJoinKey.of(keyColumn, viableKeyColumns);
    }

    static JoinKey syntheticColumn(final Expression leftJoinExp, final Expression rightJoinExp) {
      return SyntheticJoinKey.of(leftJoinExp, rightJoinExp);
    }

    /**
     * @return {@code true} if the join key is synthetic.
     */
    boolean isSynthetic();

    /**
     * @return the list of all viable key expressions.
     */
    List<? extends Expression> getAllViableKeys(LogicalSchema schema);

    /**
     * @return the list of viable key expressions, without any rewriting applied.
     */
    List<? extends Expression> getOriginalViableKeys();

    /**
     * @return Given the left and right schemas, the name of the join key.
     */
    ColumnName resolveKeyName(LogicalSchema left, LogicalSchema right);

    /**
     * Called to potentially resolve a {@link JoinKeyUdf} expression into the synthetic column
     * name.
     *
     * @param expression the expression to resolve.
     * @param schema the joined schema.
     * @return the resolved expression.
     */
    Expression resolveSelect(Expression expression, LogicalSchema schema);

    /**
     * Rewrite the join key with the supplied plugin.
     *
     * @param plugin the plugin to use.
     * @return the rewritten join key.
     */
    JoinKey rewriteWith(BiFunction<Expression, Context<Void>, Optional<Expression>> plugin);
  }

  public static final class SourceJoinKey implements JoinKey {

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
    public List<? extends Expression> getAllViableKeys(final LogicalSchema schema) {
      return ImmutableList.<Expression>builder()
          .addAll(viableKeyColumns)
          .addAll(originalViableKeyColumns)
          .build();
    }

    @Override
    public List<? extends Expression> getOriginalViableKeys() {
      return originalViableKeyColumns;
    }

    @Override
    public ColumnName resolveKeyName(final LogicalSchema left, final LogicalSchema right) {
      return keyColumn;
    }

    @Override
    public Expression resolveSelect(final Expression expression, final LogicalSchema schema) {
      return expression;
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

  public static final class SyntheticJoinKey implements JoinKey {

    private final FunctionCall originalJoinKeyUdf;
    private final FunctionCall joinKeyUdf;

    static JoinKey of(final Expression leftJoinExp, final Expression rightJoinExp) {
      final FunctionCall udf = new FunctionCall(
          JoinKeyUdf.NAME,
          ImmutableList.of(leftJoinExp, rightJoinExp)
      );

      return new SyntheticJoinKey(udf, udf);
    }

    private SyntheticJoinKey(final FunctionCall originalJoinKeyUdf, final FunctionCall joinKeyUdf) {
      this.originalJoinKeyUdf = requireNonNull(originalJoinKeyUdf, "originalJoinKeyUdf");
      this.joinKeyUdf = requireNonNull(joinKeyUdf, "joinKeyUdf");
    }

    @Override
    public boolean isSynthetic() {
      return true;
    }

    @Override
    public List<? extends Expression> getAllViableKeys(final LogicalSchema schema) {
      return ImmutableList.<Expression>builder()
          .add(joinKeyUdf)
          .add(originalJoinKeyUdf)
          .add(new UnqualifiedColumnReferenceExp(Iterables.getOnlyElement(schema.key()).name()))
          .build();
    }

    @Override
    public List<? extends Expression> getOriginalViableKeys() {
      return ImmutableList.of(originalJoinKeyUdf);
    }

    @Override
    public ColumnName resolveKeyName(final LogicalSchema left, final LogicalSchema right) {
      return ColumnNames.nextKsqlColAlias(left, right);
    }

    @Override
    public Expression resolveSelect(
        final Expression expression,
        final LogicalSchema schema
    ) {
      return joinKeyUdf.equals(expression)
          ? new UnqualifiedColumnReferenceExp(Iterables.getOnlyElement(schema.key()).name())
          : expression;
    }

    @Override
    public JoinKey rewriteWith(
        final BiFunction<Expression, Context<Void>, Optional<Expression>> plugin
    ) {
      final FunctionCall rewritten = ExpressionTreeRewriter
          .rewriteWith(plugin, joinKeyUdf);

      return new SyntheticJoinKey(originalJoinKeyUdf, rewritten);
    }
  }
}
