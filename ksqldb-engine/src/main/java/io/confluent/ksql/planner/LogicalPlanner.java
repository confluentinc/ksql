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

import com.google.common.collect.Iterables;
import io.confluent.ksql.analyzer.AggregateAnalysisResult;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.analyzer.Analysis.Into.NewTopic;
import io.confluent.ksql.analyzer.Analysis.JoinInfo;
import io.confluent.ksql.analyzer.FilterTypeValidator;
import io.confluent.ksql.analyzer.FilterTypeValidator.FilterType;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
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
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.function.udf.AsValue;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.planner.JoinTree.Join;
import io.confluent.ksql.planner.JoinTree.Leaf;
import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.FinalProjectNode;
import io.confluent.ksql.planner.plan.FlatMapNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.JoinNode.JoinKey;
import io.confluent.ksql.planner.plan.JoinNode.JoinType;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.planner.plan.PreJoinProjectNode;
import io.confluent.ksql.planner.plan.PreJoinRepartitionNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.QueryFilterNode;
import io.confluent.ksql.planner.plan.QueryProjectNode;
import io.confluent.ksql.planner.plan.SelectionUtil;
import io.confluent.ksql.planner.plan.SingleSourcePlanNode;
import io.confluent.ksql.planner.plan.SuppressNode;
import io.confluent.ksql.planner.plan.UserRepartitionNode;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.SerdeFeaturesFactory;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.serde.none.NoneFormat;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
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
  public OutputNode buildPersistentLogicalPlan() {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    final boolean isWindowed = analysis
        .getFrom()
        .getDataSource()
        .getKsqlTopic()
        .getKeyFormat().isWindowed();

    PlanNode currentNode = buildSourceNode(isWindowed);

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

  public OutputNode buildPullLogicalPlan(final PullPlannerOptions pullPlannerOptions) {
    final boolean isWindowed = analysis
        .getFrom()
        .getDataSource()
        .getKsqlTopic()
        .getKeyFormat().isWindowed();

    PlanNode currentNode = buildSourceNode(isWindowed);

    if (analysis.getWhereExpression().isPresent()) {
      final Expression whereExpression = analysis.getWhereExpression().get();
      final FilterTypeValidator validator = new FilterTypeValidator(
          currentNode.getSchema(),
          metaStore,
          FilterType.WHERE);

      validator.validateFilterExpression(whereExpression);

      currentNode = new QueryFilterNode(
          new PlanNodeId("WhereFilter"),
          currentNode,
          whereExpression,
          metaStore,
          ksqlConfig,
          isWindowed,
          pullPlannerOptions);
    } else {
      if (!pullPlannerOptions.getTableScansEnabled()) {
        throw QueryFilterNode.invalidWhereClauseException("Missing WHERE clause", isWindowed);
      }
    }

    currentNode = new QueryProjectNode(
        new PlanNodeId("Project"),
        currentNode,
        analysis.getSelectItems(),
        metaStore,
        ksqlConfig,
        analysis,
        isWindowed,
        pullPlannerOptions);

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
          timestampColumn,
          getWindowInfo()
      );
    }

    final Into into = analysis.getInto().get();

    final KsqlTopic existingTopic = getSinkTopic(into, sourcePlanNode.getSchema());

    return new KsqlStructuredDataOutputNode(
        new PlanNodeId(into.getName().text()),
        sourcePlanNode,
        inputSchema,
        timestampColumn,
        existingTopic,
        analysis.getLimitClause(),
        into.isCreate(),
        into.getName(),
        analysis.getOrReplace()
    );
  }

  private Optional<WindowInfo> getWindowInfo() {
    final KsqlTopic srcTopic = analysis
        .getFrom()
        .getDataSource()
        .getKsqlTopic();

    final Optional<WindowInfo> explicitWindowInfo = analysis.getWindowExpression()
        .map(WindowExpression::getKsqlWindowExpression)
        .map(KsqlWindowExpression::getWindowInfo);

    return explicitWindowInfo.isPresent()
        ? explicitWindowInfo
        : srcTopic.getKeyFormat().getWindowInfo();
  }

  private KsqlTopic getSinkTopic(final Into into, final LogicalSchema schema) {
    if (into.getExistingTopic().isPresent()) {
      return into.getExistingTopic().get();
    }

    final NewTopic newTopic = into.getNewTopic().orElseThrow(IllegalStateException::new);
    final FormatInfo keyFormat = getSinkKeyFormat(schema, newTopic);

    final SerdeFeatures keyFeatures = SerdeFeaturesFactory.buildKeyFeatures(
        schema,
        FormatFactory.of(keyFormat)
    );

    final SerdeFeatures valFeatures = SerdeFeaturesFactory.buildValueFeatures(
        schema,
        FormatFactory.of(newTopic.getValueFormat()),
        analysis.getProperties().getValueSerdeFeatures(),
        ksqlConfig
    );

    return new KsqlTopic(
        newTopic.getTopicName(),
        KeyFormat.of(keyFormat, keyFeatures, newTopic.getWindowInfo()),
        ValueFormat.of(newTopic.getValueFormat(), valFeatures)
    );
  }

  private FormatInfo getSinkKeyFormat(final LogicalSchema schema, final NewTopic newTopic) {
    // If the inherited key format is NONE, and the result has key columns, use default key format:
    final boolean resultHasKeyColumns = !schema.key().isEmpty();
    final boolean inheritedNone = !analysis.getProperties().getKeyFormat().isPresent()
        && newTopic.getKeyFormat().getFormat().equals(NoneFormat.NAME);

    if (!inheritedNone || !resultHasKeyColumns) {
      return newTopic.getKeyFormat();
    }

    final String defaultKeyFormat = ksqlConfig.getString(KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG);
    return FormatInfo.of(defaultKeyFormat);
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
        .map(DataSource::getSchema);
  }

  private AggregateNode buildAggregateNode(final PlanNode sourcePlanNode) {
    final GroupBy groupBy = analysis.getGroupBy()
        .orElseThrow(IllegalStateException::new);

    final List<SelectExpression> projectionExpressions = SelectionUtil.buildSelectExpressions(
        sourcePlanNode,
        analysis.getSelectItems(),
        getTargetSchema()
    );

    final RewrittenAggregateAnalysis aggregateAnalysis = new RewrittenAggregateAnalysis(
        aggregateAnalyzer.analyze(analysis, projectionExpressions),
        refRewriter::process
    );

    final LogicalSchema schema =
        buildAggregateSchema(sourcePlanNode, groupBy, projectionExpressions);

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
        analysis.getInto().isPresent(),
        ksqlConfig
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

  private UserRepartitionNode buildUserRepartitionNode(
      final PlanNode currentNode,
      final PartitionBy partitionBy
  ) {
    final List<Expression> rewrittenPartitionBys = partitionBy.getExpressions().stream()
        .map(exp -> ExpressionTreeRewriter.rewriteWith(refRewriter::process, exp))
        .collect(Collectors.toList());

    final LogicalSchema schema =
        buildRepartitionedSchema(currentNode, rewrittenPartitionBys);

    return new UserRepartitionNode(
        new PlanNodeId("PartitionBy"),
        currentNode,
        schema,
        partitionBy.getExpressions(),
        rewrittenPartitionBys
    );
  }

  private PreJoinRepartitionNode buildInternalRepartitionNode(
      final PlanNode source,
      final String side,
      final Expression joinExpression,
      final BiFunction<Expression, Context<Void>, Optional<Expression>> plugin
  ) {
    final Expression rewrittenPartitionBy =
        ExpressionTreeRewriter.rewriteWith(plugin, joinExpression);

    final LogicalSchema schema =
        buildRepartitionedSchema(source, Collections.singletonList(rewrittenPartitionBy));

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

  private PlanNode prepareSourceForJoin(
      final JoinTree.Node node,
      final PlanNode joinSource,
      final String side,
      final Expression joinExpression,
      final boolean isForeignKeyJoin
  ) {
    if (node instanceof JoinTree.Join) {
      return prepareSourceForJoin(
          (JoinTree.Join) node,
          joinSource,
          side,
          joinExpression,
          isForeignKeyJoin
      );
    } else {
      return prepareSourceForJoin(
          (DataSourceNode) joinSource,
          side,
          joinExpression,
          isForeignKeyJoin
      );
    }
  }

  private PlanNode prepareSourceForJoin(
      final DataSourceNode sourceNode,
      final String side,
      final Expression joinExpression,
      final boolean isForeignKeyJoin
  ) {
    final PlanNode preProjectNode;
    if (isForeignKeyJoin) {
      // we do not need to repartition for foreign key joins, as FK joins do not
      // have co-partitioning requirements
      preProjectNode = sourceNode;
    } else {
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

      preProjectNode =
          buildInternalRepartitionNode(sourceNode, side, joinExpression, rewriter::process);
    }

    return buildInternalProjectNode(
        preProjectNode,
        "PrependAlias" + side,
        sourceNode.getAlias()
    );
  }

  private PlanNode prepareSourceForJoin(
      final Join join,
      final PlanNode joinedSource,
      final String side,
      final Expression joinExpression,
      final boolean isForeignKeyJoin
  ) {
    // we do not need to repartition for foreign key joins, as FK joins do not
    // have co-partitioning requirements
    if (isForeignKeyJoin) {
      return joinedSource;
    }

    // we do not need to repartition if the joinExpression
    // is already part of the join equivalence set
    if (join.joinEquivalenceSet().contains(joinExpression)) {
      return joinedSource;
    }

    return buildInternalRepartitionNode(joinedSource, side, joinExpression, refRewriter::process);
  }

  private PlanNode buildSourceNode(final boolean isWindowed) {
    if (!analysis.isJoin()) {
      return buildNonJoinNode(analysis.getFrom(), isWindowed);
    }

    final List<JoinInfo> joinInfo = analysis.getJoin();
    final JoinTree.Node tree = JoinTree.build(joinInfo);
    if (tree instanceof JoinTree.Leaf) {
      throw new IllegalStateException("Expected more than one source:"
          + analysis.getAllDataSources());
    }

    final JoinNode joinNode = buildJoin((Join) tree, "", isWindowed);
    joinNode.resolveKeyFormats();
    return joinNode;
  }

  /**
   * @param root    the root of the Join Tree
   * @param prefix  the prefix to uniquely identify the plan node
   * @return the PlanNode representing this Join Tree
   */
  private JoinNode buildJoin(final Join root, final String prefix, final boolean isWindowed) {
    final PlanNode preRepartitionLeft;
    if (root.getLeft() instanceof JoinTree.Join) {
      preRepartitionLeft = buildJoin((Join) root.getLeft(), prefix + "L_", isWindowed);
    } else {
      final JoinTree.Leaf leaf = (Leaf) root.getLeft();
      preRepartitionLeft = new DataSourceNode(
          new PlanNodeId("KafkaTopic_" + prefix + "Left"),
          leaf.getSource().getDataSource(),
          leaf.getSource().getAlias(),
          isWindowed
      );
    }

    final PlanNode preRepartitionRight;
    if (root.getRight() instanceof JoinTree.Join) {
      preRepartitionRight = buildJoin((Join) root.getRight(), prefix + "R_", isWindowed);
    } else {
      final JoinTree.Leaf leaf = (Leaf) root.getRight();
      preRepartitionRight = new DataSourceNode(
          new PlanNodeId("KafkaTopic_" + prefix + "Right"),
          leaf.getSource().getDataSource(),
          leaf.getSource().getAlias(),
          isWindowed
      );
    }

    final boolean isForeignKeyJoin =
        verifyJoin(root.getInfo(), preRepartitionLeft, preRepartitionRight);

    final boolean finalJoin = prefix.isEmpty();

    final JoinKey joinKey = buildJoinKey(root);

    final PlanNode left = prepareSourceForJoin(
        root.getLeft(),
        preRepartitionLeft,
        prefix + "Left",
        root.getInfo().getLeftJoinExpression(),
        isForeignKeyJoin
    );
    final PlanNode right = prepareSourceForJoin(
        root.getRight(),
        preRepartitionRight,
        prefix + "Right",
        root.getInfo().getRightJoinExpression(),
        isForeignKeyJoin
    );

    return new JoinNode(
        new PlanNodeId(prefix + "Join"),
        root.getInfo().getType(),
        joinKey.rewriteWith(refRewriter::process),
        finalJoin,
        left,
        right,
        root.getInfo().getWithinExpression(),
        ksqlConfig.getString(KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG)
    );
  }

  /**
   * @return whether this is a foreign key join or not
   */
  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  private boolean verifyJoin(
      final JoinInfo joinInfo,
      final PlanNode leftNode,
      final PlanNode rightNode
  ) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    final JoinType joinType = joinInfo.getType();
    final Expression leftExpression = joinInfo.getLeftJoinExpression();
    final Expression rightExpression = joinInfo.getRightJoinExpression();

    if (leftNode.getNodeOutputType() == DataSourceType.KSTREAM) {
      if (rightNode.getNodeOutputType() == DataSourceType.KTABLE) {

        // stream-table join detected

        if (joinType.equals(JoinType.OUTER)) {
          throw new KsqlException(String.format(
                  "Invalid join type: "
                      + "full-outer join not supported for stream-table join. Got %s %s %s.",
                  joinInfo.getLeftSource().getDataSource().getName().text(),
                  joinType,
                  joinInfo.getRightSource().getDataSource().getName().text()
              ));
        }

        if (joinOnNonKeyAttribute(rightExpression, rightNode)) {
          throw new KsqlException(String.format(
                  "Invalid join condition:"
                      + " stream-table joins require to join on the table's primary key."
                      + " Got %s = %s.",
                  leftExpression,
                  rightExpression
              ));
        }
      }

      // stream-stream join detected: nothing to verify

    } else {
      if (rightNode.getNodeOutputType() == DataSourceType.KSTREAM) {
        throw new KsqlException(String.format(
                "Invalid join order:"
                  + " table-stream joins are not supported; only stream-table joins. Got %s %s %s.",
                joinInfo.getLeftSource().getDataSource().getName().text(),
                joinType,
                joinInfo.getRightSource().getDataSource().getName().text()
            ));
      }

      // table-table join detected

      if (joinOnNonKeyAttribute(rightExpression, rightNode)) {
        throw new KsqlException(String.format(
                "Invalid join condition:"
                    + " table-table joins require to join on the primary key of the right input"
                    + " table. Got %s = %s.",
                leftExpression,
                rightExpression
           ));
      }

      if (joinOnNonKeyAttribute(leftExpression, leftNode)) {
        // foreign key join detected

        if (ksqlConfig.getBoolean(KsqlConfig.KSQL_FOREIGN_KEY_JOINS_ENABLED)) {

          // after we lift this n-way join restriction, we should be able to support FK-joins
          // at any level in the join tree, even after we add right-deep/bushy join tree support,
          // because a FK-join output table has the same PK as its left input table
          if (!(leftNode instanceof DataSourceNode)
              || !(rightNode instanceof DataSourceNode)) {
            throw new KsqlException(String.format(
                "Invalid join condition: foreign-key table-table joins are not "
                    + "supported as part of n-way joins. Got %s = %s.",
                leftExpression,
                rightExpression
            ));
          }

          return true;
        } else {
          throw new KsqlException(String.format(
              "Invalid join condition:"
                  + " foreign-key table-table joins are not supported. Got %s = %s.",
              leftExpression,
              rightExpression
          ));
        }
      }
    }

    return false;
  }

  private boolean joinOnNonKeyAttribute(final Expression joinExpression,
                                        final PlanNode node) {
    if (!(joinExpression instanceof ColumnReferenceExp)) {
      return true;
    }
    final ColumnReferenceExp simpleJoinExpression = (ColumnReferenceExp) joinExpression;

    final ColumnName joinAttributeName = simpleJoinExpression.getColumnName();
    final List<DataSourceNode> dataSourceNodes = node.getSourceNodes().collect(Collectors.toList());
    final List<Column> keyColumns;

    // n-way join sub-tree (ie, not a leaf)
    if (isInnerNode(node)) {
      final DataSourceNode qualifiedNode;

      if (simpleJoinExpression.maybeQualifier().isPresent()) {
        final SourceName qualifier = simpleJoinExpression.maybeQualifier().get();
        final List<DataSourceNode> allNodes = dataSourceNodes.stream()
            .filter(n -> n.getDataSource().getName().equals(qualifier))
            .collect(Collectors.toList());

        if (allNodes.size() != 1) {
          throw new KsqlException(String.format(
              "Join qualifier '%s' could not be resolved (either not found or not unique).",
              qualifier
          ));
        }
        qualifiedNode = Iterables.getOnlyElement(allNodes);
      } else {
        final List<DataSourceNode> allNodes = dataSourceNodes.stream()
            .filter(n -> n.getSchema().findColumn(simpleJoinExpression.getColumnName()).isPresent())
            .collect(Collectors.toList());

        if (allNodes.size() != 1) {
          throw new KsqlException(String.format(
              "Join identifier '%s' could not be resolved (either not found or not unique).",
              joinAttributeName
          ));
        }
        qualifiedNode = Iterables.getOnlyElement(allNodes);
      }

      keyColumns = qualifiedNode.getSchema().key();
    } else {
      // leaf node: we know we have single data source
      keyColumns = Iterables.getOnlyElement(dataSourceNodes).getSchema().key();
    }

    // we only support joins on single attributes
    // - the given join expression is checked upfront, and thus we know it refers to a single column
    // - thus, if the key has more than one column, the join is not on the key
    if (keyColumns.size() > 1) {
      return true;
    }

    return !joinAttributeName.equals(Iterables.getOnlyElement(keyColumns).name());
  }

  private boolean isInnerNode(final PlanNode node) {
    if (node instanceof JoinNode) {
      return true;
    }

    if (node instanceof DataSourceNode) {
      return false;
    }

    if (node instanceof SingleSourcePlanNode) {
      return isInnerNode(((SingleSourcePlanNode) node).getSource());
    }

    throw new IllegalStateException("Unknown node type: " + node.getClass().getName());
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

  private static DataSourceNode buildNonJoinNode(
      final AliasedDataSource dataSource, final boolean isWindowed) {
    return new DataSourceNode(
        new PlanNodeId("KsqlTopic"),
        dataSource.getDataSource(),
        dataSource.getAlias(),
        isWindowed
    );
  }

  private static SuppressNode buildSuppressNode(
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

    final ExpressionTypeManager typeManager =
        new ExpressionTypeManager(sourceSchema, metaStore);

    for (final Expression expression : groupByExps) {
      final SqlType keyType = typeManager.getExpressionSqlType(expression);
      final ColumnName keyName = selectResolver.apply(expression)
          .orElseGet(() -> expression instanceof ColumnReferenceExp
              ? ((ColumnReferenceExp) expression).getColumnName()
              : ColumnNames.uniqueAliasFor(expression, sourceSchema)
          );

      builder.keyColumn(keyName, keyType);
    }

    return builder
        .valueColumns(valueColumns)
        .build();
  }

  private LogicalSchema buildRepartitionedSchema(
      final PlanNode sourceNode,
      final List<Expression> partitionBys
  ) {
    final LogicalSchema sourceSchema = sourceNode.getSchema();

    return PartitionByParamsFactory.buildSchema(
        sourceSchema,
        partitionBys,
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
