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

package io.confluent.ksql.planner;

import io.confluent.ksql.analyzer.AggregateAnalysisResult;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.analyzer.Analysis.JoinInfo;
import io.confluent.ksql.analyzer.FilterTypeValidator;
import io.confluent.ksql.analyzer.FilterTypeValidator.FilterType;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.PartitionByParamsFactory;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicyFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.udf.AsValue;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.planner.JoinTree.Join;
import io.confluent.ksql.planner.JoinTree.Leaf;
import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.FinalProjectNode;
import io.confluent.ksql.planner.plan.FlatMapNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.JoinNode.JoinKey;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.planner.plan.PreJoinProjectNode;
import io.confluent.ksql.planner.plan.PreJoinRepartitionNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.RepartitionNode;
import io.confluent.ksql.planner.plan.SelectionUtil;
import io.confluent.ksql.planner.plan.SuppressNode;
import io.confluent.ksql.planner.plan.UserRepartitionNode;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.SerdeOptionsFactory;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class LogicalPlanner {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final KsqlConfig ksqlConfig;
  private final RewrittenAnalysis analysis;
  private final MetaStore metaStore;
  private final AggregateAnalyzer aggregateAnalyzer;
  private final ColumnReferenceRewriter refRewriter;

  public LogicalPlanner(
      final KsqlConfig ksqlConfig,
      final ImmutableAnalysis analysis,
      final MetaStore metaStore
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.refRewriter =
        new ColumnReferenceRewriter(analysis.getFromSourceSchemas(false).isJoin());
    this.analysis = new RewrittenAnalysis(analysis, refRewriter::process);
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.aggregateAnalyzer = new AggregateAnalyzer(metaStore);
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  public OutputNode buildPlan() {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    PlanNode currentNode = buildSourceNode();

    if (analysis.getWhereExpression().isPresent()) {
      currentNode = buildFilterNode(currentNode, analysis.getWhereExpression().get());
    }

    if (analysis.original().getPartitionBy().isPresent()) {
      currentNode = buildUserRepartitionNode(
          currentNode,
          analysis.original().getPartitionBy().get()
      );
    }

    if (!analysis.getTableFunctions().isEmpty()) {
      currentNode = buildFlatMapNode(currentNode);
    }

    if (analysis.getGroupBy().isPresent()) {
      currentNode = buildAggregateNode(currentNode);
    } else {
      if (analysis.getWindowExpression().isPresent()) {
        final String loc = analysis.getWindowExpression().get()
            .getLocation()
            .map(NodeLocation::asPrefix)
            .orElse("");
        throw new KsqlException(loc + "WINDOW clause requires a GROUP BY clause.");
      }
      currentNode = buildUserProjectNode(currentNode);
    }

    if (analysis.getRefinementInfo().isPresent()
        && analysis.getRefinementInfo().get().getOutputRefinement() == OutputRefinement.FINAL) {
      if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_SUPPRESS_ENABLED)) {
        throw new KsqlException("Suppression is currently disabled. You can enable it by setting "
            + KsqlConfig.KSQL_SUPPRESS_ENABLED + " to true");
      }
      if (!(analysis.getGroupBy().isPresent() && analysis.getWindowExpression().isPresent())) {
        throw new KsqlException("EMIT FINAL is only supported for windowed aggregations.");
      }
      currentNode = buildSuppressNode(
          currentNode,
          analysis.getRefinementInfo().get()
      );
    }

    return buildOutputNode(currentNode);
  }

  private OutputNode buildOutputNode(final PlanNode sourcePlanNode) {
    final LogicalSchema inputSchema = sourcePlanNode.getSchema();
    final Optional<TimestampColumn> timestampColumn = getTimestampColumn(inputSchema, analysis);

    if (!analysis.getInto().isPresent()) {
      return new KsqlBareOutputNode(
          new PlanNodeId("KSQL_STDOUT_NAME"),
          sourcePlanNode,
          inputSchema,
          analysis.getLimitClause(),
          timestampColumn
      );
    }

    final Into intoDataSource = analysis.getInto().get();

    return new KsqlStructuredDataOutputNode(
        new PlanNodeId(intoDataSource.getName().text()),
        sourcePlanNode,
        inputSchema,
        timestampColumn,
        intoDataSource.getKsqlTopic(),
        analysis.getLimitClause(),
        intoDataSource.isCreate(),
        getSerdeOptions(sourcePlanNode, intoDataSource),
        intoDataSource.getName()
    );
  }

  private SerdeOptions getSerdeOptions(
      final PlanNode sourcePlanNode,
      final Into intoDataSource
  ) {
    final List<ColumnName> columnNames = sourcePlanNode.getSchema().value().stream()
        .map(Column::name)
        .collect(Collectors.toList());

    final Format valueFormat = intoDataSource.getKsqlTopic()
        .getValueFormat()
        .getFormat();

    return SerdeOptionsFactory.buildForCreateAsStatement(
        columnNames,
        valueFormat,
        analysis.getProperties().getSerdeOptions(),
        ksqlConfig
    );
  }

  private Optional<TimestampColumn> getTimestampColumn(
      final LogicalSchema inputSchema,
      final ImmutableAnalysis analysis
  ) {
    final Optional<ColumnName> timestampColumnName =
        analysis.getProperties().getTimestampColumnName();
    final Optional<TimestampColumn> timestampColumn = timestampColumnName.map(
        n -> new TimestampColumn(n, analysis.getProperties().getTimestampFormat())
    );
    TimestampExtractionPolicyFactory.validateTimestampColumn(
        ksqlConfig,
        inputSchema,
        timestampColumn
    );
    return timestampColumn;
  }

  private Optional<LogicalSchema> getTargetSchema() {
    return analysis.getInto().filter(i -> !i.isCreate())
        .map(i -> metaStore.getSource(i.getName()))
        .map(target -> target.getSchema());
  }

  private AggregateNode buildAggregateNode(final PlanNode sourcePlanNode) {
    final GroupBy groupBy = analysis.getGroupBy()
        .orElseThrow(IllegalStateException::new);

    final List<SelectExpression> projectionExpressions = SelectionUtil.buildSelectExpressions(
        sourcePlanNode,
        analysis.getSelectItems(),
        getTargetSchema()
    );

    final LogicalSchema schema =
        buildAggregateSchema(sourcePlanNode, groupBy, projectionExpressions);

    final RewrittenAggregateAnalysis aggregateAnalysis = new RewrittenAggregateAnalysis(
        aggregateAnalyzer.analyze(analysis, projectionExpressions),
        refRewriter::process
    );

    if (analysis.getHavingExpression().isPresent()) {
      final FilterTypeValidator validator = new FilterTypeValidator(
          sourcePlanNode.getSchema(),
          metaStore,
          FilterType.HAVING);

      validator.validateFilterExpression(analysis.getHavingExpression().get());
    }

    return new AggregateNode(
        new PlanNodeId("Aggregate"),
        sourcePlanNode,
        schema,
        groupBy,
        metaStore,
        analysis,
        aggregateAnalysis,
        projectionExpressions,
        analysis.getInto().isPresent()
    );
  }

  private ProjectNode buildUserProjectNode(final PlanNode parentNode) {
    return new FinalProjectNode(
        new PlanNodeId("Project"),
        parentNode,
        analysis.getSelectItems(),
        analysis.getInto(),
        metaStore
    );
  }

  private static ProjectNode buildInternalProjectNode(
      final PlanNode parent,
      final String id,
      final SourceName sourceAlias
  ) {
    return new PreJoinProjectNode(
        new PlanNodeId(id),
        parent,
        sourceAlias
    );
  }

  private FilterNode buildFilterNode(
      final PlanNode sourcePlanNode,
      final Expression filterExpression
  ) {
    final FilterTypeValidator validator = new FilterTypeValidator(
        sourcePlanNode.getSchema(),
        metaStore,
        FilterType.WHERE);

    validator.validateFilterExpression(filterExpression);

    return new FilterNode(new PlanNodeId("WhereFilter"), sourcePlanNode, filterExpression);
  }

  private RepartitionNode buildUserRepartitionNode(
      final PlanNode currentNode,
      final PartitionBy partitionBy
  ) {
    final Expression rewrittenPartitionBy =
        ExpressionTreeRewriter.rewriteWith(refRewriter::process, partitionBy.getExpression());

    final LogicalSchema schema =
        buildRepartitionedSchema(currentNode, rewrittenPartitionBy);

    return new UserRepartitionNode(
        new PlanNodeId("PartitionBy"),
        currentNode,
        schema,
        partitionBy.getExpression(),
        rewrittenPartitionBy
    );
  }

  private RepartitionNode buildInternalRepartitionNode(
      final PlanNode source,
      final String side,
      final Expression joinExpression,
      final BiFunction<Expression, Context<Void>, Optional<Expression>> plugin
  ) {
    final Expression rewrittenPartitionBy =
        ExpressionTreeRewriter.rewriteWith(plugin, joinExpression);

    final LogicalSchema schema =
        buildRepartitionedSchema(source, rewrittenPartitionBy);

    return new PreJoinRepartitionNode(
        new PlanNodeId(side + "SourceKeyed"),
        source,
        schema,
        rewrittenPartitionBy
    );
  }

  private FlatMapNode buildFlatMapNode(final PlanNode sourcePlanNode) {
    return new FlatMapNode(new PlanNodeId("FlatMap"), sourcePlanNode, metaStore, analysis);
  }

  private PlanNode buildSourceForJoin(
      final AliasedDataSource source,
      final String side,
      final Expression joinExpression
  ) {
    final DataSourceNode sourceNode = new DataSourceNode(
        new PlanNodeId("KafkaTopic_" + side),
        source.getDataSource(),
        source.getAlias()
    );

    // it is always safe to build the repartition node - this operation will be
    // a no-op if a repartition is not required. if the source is a table, and
    // a repartition is needed, then an exception will be thrown
    final VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> rewriter =
        new VisitParentExpressionVisitor<Optional<Expression>, Context<Void>>(Optional.empty()) {
          @Override
          public Optional<Expression> visitQualifiedColumnReference(
              final QualifiedColumnReferenceExp node,
              final Context<Void> ctx
          ) {
            return Optional.of(new UnqualifiedColumnReferenceExp(node.getColumnName()));
          }
        };

    final PlanNode repartition =
        buildInternalRepartitionNode(sourceNode, side, joinExpression, rewriter::process);

    return buildInternalProjectNode(
        repartition,
        "PrependAlias" + side,
        source.getAlias()
    );
  }

  private PlanNode buildSourceForJoin(
      final Join join,
      final PlanNode joinedSource,
      final String side,
      final Expression joinExpression
  ) {
    // we do not need to repartition if the joinExpression
    // is already part of the join equivalence set
    if (join.joinEquivalenceSet().contains(joinExpression)) {
      return joinedSource;
    }

    return buildInternalRepartitionNode(joinedSource, side, joinExpression, refRewriter::process);
  }

  private PlanNode buildSourceNode() {
    if (!analysis.isJoin()) {
      return buildNonJoinNode(analysis.getFrom());
    }

    final List<JoinInfo> joinInfo = analysis.getJoin();
    final JoinTree.Node tree = JoinTree.build(joinInfo);
    if (tree instanceof JoinTree.Leaf) {
      throw new IllegalStateException("Expected more than one source:"
          + analysis.getAllDataSources());
    }

    return buildJoin((Join) tree, "");
  }

  /**
   * @param root    the root of the Join Tree
   * @param prefix  the prefix to uniquely identify the plan node
   * @return the PlanNode representing this Join Tree
   */
  private PlanNode buildJoin(final Join root, final String prefix) {
    final PlanNode left;
    if (root.getLeft() instanceof JoinTree.Join) {
      left = buildSourceForJoin(
          (JoinTree.Join) root.getLeft(),
          buildJoin((Join) root.getLeft(), prefix + "L_"),
          prefix + "Left",
          root.getInfo().getLeftJoinExpression()
      );
    } else {
      final JoinTree.Leaf leaf = (Leaf) root.getLeft();
      left = buildSourceForJoin(
          leaf.getSource(), prefix + "Left", root.getInfo().getLeftJoinExpression());
    }

    final PlanNode right;
    if (root.getRight() instanceof JoinTree.Join) {
      right = buildSourceForJoin(
          (JoinTree.Join) root.getRight(),
          buildJoin((Join) root.getRight(), prefix + "R_"),
          prefix + "Right",
          root.getInfo().getRightJoinExpression()
      );
    } else {
      final JoinTree.Leaf leaf = (Leaf) root.getRight();
      right = buildSourceForJoin(
          leaf.getSource(), prefix + "Right", root.getInfo().getRightJoinExpression());
    }

    final boolean finalJoin = prefix.isEmpty();

    final JoinKey joinKey = buildJoinKey(root);

    return new JoinNode(
        new PlanNodeId(prefix + "Join"),
        root.getInfo().getType(),
        joinKey.rewriteWith(refRewriter::process),
        finalJoin,
        left,
        right,
        root.getInfo().getWithinExpression()
    );
  }

  private JoinKey buildJoinKey(final Join join) {
    final List<QualifiedColumnReferenceExp> viableKeyColumns = join.viableKeyColumns();
    if (viableKeyColumns.isEmpty()) {
      return JoinKey.syntheticColumn();
    }

    final Projection projection = Projection.of(analysis.original().getSelectItems());

    final List<QualifiedColumnReferenceExp> availableKeyColumns = viableKeyColumns.stream()
        .filter(projection::containsExpression)
        .collect(Collectors.toList());

    final QualifiedColumnReferenceExp keyColumn = availableKeyColumns.isEmpty()
        ? viableKeyColumns.get(0) // Lack of availability is handle later.
        : availableKeyColumns.get(0);

    final ColumnName keyColumnName = ColumnNames
        .generatedJoinColumnAlias(keyColumn.getQualifier(), keyColumn.getColumnName());

    return JoinKey.sourceColumn(keyColumnName, viableKeyColumns);
  }

  private static DataSourceNode buildNonJoinNode(final AliasedDataSource dataSource) {
    return new DataSourceNode(
        new PlanNodeId("KsqlTopic"),
        dataSource.getDataSource(),
        dataSource.getAlias()
    );
  }

  private SuppressNode buildSuppressNode(
      final PlanNode sourcePlanNode,
      final RefinementInfo refinementInfo
  ) {
    return new SuppressNode(
        new PlanNodeId("Suppress"),
        sourcePlanNode,
        refinementInfo
    );
  }

  private LogicalSchema buildAggregateSchema(
      final PlanNode sourcePlanNode,
      final GroupBy groupBy,
      final List<SelectExpression> projectionExpressions
  ) {
    final LogicalSchema sourceSchema = sourcePlanNode.getSchema();

    final LogicalSchema projectionSchema = SelectionUtil.buildProjectionSchema(
        sourceSchema
            .withPseudoAndKeyColsInValue(analysis.getWindowExpression().isPresent()),
        projectionExpressions,
        metaStore
    );

    final List<Expression> groupByExps = groupBy.getGroupingExpressions();

    final Function<Expression, Optional<ColumnName>> selectResolver = expression -> {
      final List<ColumnName> foundInProjection = projectionExpressions.stream()
          .filter(e -> e.getExpression().equals(expression))
          .map(SelectExpression::getAlias)
          .collect(Collectors.toList());

      switch (foundInProjection.size()) {
        case 0:
          return Optional.empty();

        case 1:
          return Optional.of(foundInProjection.get(0));

        default:
          final String keys = GrammaticalJoiner.and().join(foundInProjection);
          throw new KsqlException("The projection contains a key column more than once: " + keys
              + "."
              + System.lineSeparator()
              + "Each key column must only be in the projection once. "
              + "If you intended to copy the key into the value, then consider using the "
              + AsValue.NAME + " function to indicate which key reference should be copied."
          );
      }
    };

    final ColumnName keyName;
    final SqlType keyType;

    if (groupByExps.size() != 1) {
      keyType = SqlTypes.STRING;

      keyName = ColumnNames.nextKsqlColAlias(
          sourceSchema,
          LogicalSchema.builder()
              .valueColumns(projectionSchema.value())
              .build()
      );
    } else {
      final ExpressionTypeManager typeManager =
          new ExpressionTypeManager(sourceSchema, metaStore);

      final Expression expression = groupByExps.get(0);

      keyType = typeManager.getExpressionSqlType(expression);
      keyName = selectResolver.apply(expression)
          .orElseGet(() -> expression instanceof ColumnReferenceExp
              ? ((ColumnReferenceExp) expression).getColumnName()
              : ColumnNames.uniqueAliasFor(expression, sourceSchema)
          );
    }

    final List<Column> valueColumns;
    if (analysis.getInto().isPresent()) {
      // Persistent query:
      final Set<ColumnName> keyColumnNames = groupBy.getGroupingExpressions().stream()
          .map(selectResolver)
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toSet());

      valueColumns = projectionSchema.value().stream()
          .filter(col -> !keyColumnNames.contains(col.name()))
          .collect(Collectors.toList());

      if (valueColumns.isEmpty()) {
        throw new KsqlException("The projection contains no value columns.");
      }
    } else {
      // Transient query:
      // Transient queries only return value columns, so must have key columns in the value:
      valueColumns = projectionSchema.columns();
    }

    final Builder builder = LogicalSchema.builder();

    builder.keyColumn(keyName, keyType);

    return builder
        .valueColumns(valueColumns)
        .build();
  }

  private LogicalSchema buildRepartitionedSchema(
      final PlanNode sourceNode,
      final Expression partitionBy
  ) {
    final LogicalSchema sourceSchema = sourceNode.getSchema();

    return PartitionByParamsFactory.buildSchema(
        sourceSchema,
        partitionBy,
        metaStore
    );
  }

  private static final class ColumnReferenceRewriter
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    private final boolean isJoin;

    ColumnReferenceRewriter(final boolean isJoin) {
      super(Optional.empty());
      this.isJoin = isJoin;
    }

    @Override
    public Optional<Expression> visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Context<Void> ctx
    ) {
      if (isJoin) {
        return Optional.of(new UnqualifiedColumnReferenceExp(
            node.getLocation(),
            ColumnNames.generatedJoinColumnAlias(node.getQualifier(), node.getColumnName())
        ));
      } else {
        return Optional.of(new UnqualifiedColumnReferenceExp(
            node.getLocation(),
            node.getColumnName()
        ));
      }
    }
  }

  private static final class RewrittenAggregateAnalysis implements AggregateAnalysisResult {

    private final AggregateAnalysisResult original;
    private final BiFunction<Expression, Context<Void>, Optional<Expression>> rewriter;

    private RewrittenAggregateAnalysis(
        final AggregateAnalysisResult original,
        final BiFunction<Expression, Context<Void>, Optional<Expression>> rewriter
    ) {
      this.original = Objects.requireNonNull(original, "original");
      this.rewriter = Objects.requireNonNull(rewriter, "rewriter");
    }

    @Override
    public List<Expression> getAggregateFunctionArguments() {
      return rewriteList(original.getAggregateFunctionArguments());
    }

    @Override
    public List<ColumnReferenceExp> getRequiredColumns() {
      return rewriteList(original.getRequiredColumns());
    }

    @Override
    public List<FunctionCall> getAggregateFunctions() {
      return rewriteList(original.getAggregateFunctions());
    }

    @Override
    public List<Expression> getFinalSelectExpressions() {
      return original.getFinalSelectExpressions().stream()
          .map(this::rewriteFinalSelectExpression)
          .collect(Collectors.toList());
    }

    @Override
    public Optional<Expression> getHavingExpression() {
      return rewriteOptional(original.getHavingExpression());
    }

    private Expression rewriteFinalSelectExpression(final Expression expression) {
      if (expression instanceof UnqualifiedColumnReferenceExp) {
        final ColumnName columnName = ((UnqualifiedColumnReferenceExp) expression).getColumnName();
        if (ColumnNames.isAggregate(columnName)) {
          return expression;
        }
      }

      return ExpressionTreeRewriter.rewriteWith(rewriter, expression);
    }

    private <T extends Expression> Optional<T> rewriteOptional(final Optional<T> expression) {
      return expression.map(e -> ExpressionTreeRewriter.rewriteWith(rewriter, e));
    }

    private <T extends Expression> List<T> rewriteList(final List<T> expressions) {
      return expressions.stream()
          .map(e -> ExpressionTreeRewriter.rewriteWith(rewriter, e))
          .collect(Collectors.toList());
    }
  }
}
