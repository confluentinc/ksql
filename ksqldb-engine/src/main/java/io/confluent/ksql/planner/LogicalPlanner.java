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
import io.confluent.ksql.execution.codegen.CodeGenRunner;
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
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
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
import io.confluent.ksql.planner.plan.QueryLimitNode;
import io.confluent.ksql.planner.plan.QueryProjectNode;
import io.confluent.ksql.planner.plan.SelectionUtil;
import io.confluent.ksql.planner.plan.SingleSourcePlanNode;
import io.confluent.ksql.planner.plan.UserRepartitionNode;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.SerdeFeaturesFactory;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.json.JsonSchemaFormat;
import io.confluent.ksql.serde.none.NoneFormat;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
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
import org.apache.commons.lang3.StringUtils;

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

  @SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
  public OutputNode buildPersistentLogicalPlan() {
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

    if (analysis.getGroupBy().isPresent() || !analysis.getAggregateFunctions().isEmpty()) {
      currentNode = buildAggregateNode(currentNode);
    } else {
      if (analysis.getWindowExpression().isPresent()) {
        final String loc = analysis.getWindowExpression().get()
            .getLocation()
            .map(NodeLocation::asPrefix)
            .orElse("");
        throw new KsqlException(loc + "WINDOW clause requires a GROUP BY clause.");
      }

      if (analysis.getLimitClause().isPresent()) {
        currentNode = buildLimitNode(currentNode, analysis.getLimitClause().getAsInt());
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
    }

    return buildOutputNode(currentNode);
  }

  public OutputNode buildQueryLogicalPlan(
      final QueryPlannerOptions queryPlannerOptions,
      final boolean isScalablePush
  ) {
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
          queryPlannerOptions);
    } else {
      if (!queryPlannerOptions.getTableScansEnabled()) {
        throw QueryFilterNode.invalidWhereClauseException("Missing WHERE clause", isWindowed);
      }
    }

    if (!isScalablePush && analysis.getLimitClause().isPresent()) {
      currentNode = buildLimitNode(currentNode, analysis.getLimitClause().getAsInt());
    }

    currentNode = new QueryProjectNode(
        new PlanNodeId("Project"),
        currentNode,
        analysis.getSelectItems(),
        metaStore,
        ksqlConfig,
        analysis,
        isWindowed,
        queryPlannerOptions,
        isScalablePush);

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
        ksqlConfig,
        sourcePlanNode.getSchema()
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

  private QueryLimitNode buildLimitNode(
          final PlanNode sourcePlanNode,
          final int limit
  ) {
    return new QueryLimitNode(new PlanNodeId("LimitClause"),
            sourcePlanNode, limit);
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
      final String prefix,
      final Expression joinExpression,
      final BiFunction<Expression, Context<Void>, Optional<Expression>> plugin
  ) {
    final Expression rewrittenPartitionBy =
        ExpressionTreeRewriter.rewriteWith(plugin, joinExpression);

    final LogicalSchema schema =
        buildRepartitionedSchema(source, Collections.singletonList(rewrittenPartitionBy));

    return new PreJoinRepartitionNode(
        new PlanNodeId(prefix + "SourceKeyed"),
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
      final String prefix,
      final JoinSide side,
      final Expression joinExpression,
      final boolean isForeignKeyJoin
  ) {
    if (node instanceof JoinTree.Join) {
      return prepareSourceForJoin(
          (JoinTree.Join) node,
          joinSource,
          prefix,
          side,
          joinExpression,
          isForeignKeyJoin
      );
    } else {
      return prepareSourceForJoin(
          (DataSourceNode) joinSource,
          prefix,
          side,
          joinExpression,
          isForeignKeyJoin
      );
    }
  }

  private PlanNode prepareSourceForJoin(
      final DataSourceNode sourceNode,
      final String prefix,
      final JoinSide side,
      final Expression joinExpression,
      final boolean isForeignKeyJoin
  ) {
    final PlanNode preProjectNode;
    // In the common case, we do not need to repartition foreign key joins, as FK joins do not have
    // co-partitioning requirements. However, there are corner cases covered by
    // shouldRepartitionFKJoin.
    if (isForeignKeyJoin && !shouldRepartitionFKJoin(sourceNode, side)) {
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
          buildInternalRepartitionNode(sourceNode, prefix, joinExpression, rewriter::process);
    }

    return buildInternalProjectNode(
        preProjectNode,
        "PrependAlias" + prefix,
        sourceNode.getAlias()
    );
  }

  private PlanNode prepareSourceForJoin(
      final Join join,
      final PlanNode joinedSource,
      final String prefix,
      final JoinSide side,
      final Expression joinExpression,
      final boolean isForeignKeyJoin
  ) {
    // In the common case, we do not need to repartition foreign key joins, as FK joins do not have
    // co-partitioning requirements. However, there are corner cases covered by
    // shouldRepartitionFKJoin.
    if (isForeignKeyJoin && !shouldRepartitionFKJoin(joinedSource, side)) {
      return joinedSource;
    }

    // we do not need to repartition if the joinExpression
    // is already part of the join equivalence set
    if (join.joinEquivalenceSet().contains(joinExpression)) {
      return joinedSource;
    }

    return buildInternalRepartitionNode(joinedSource, prefix, joinExpression, refRewriter::process);
  }

  private PlanNode buildSourceNode(final boolean isWindowed) {
    if (!analysis.isJoin()) {
      return buildNonJoinNode(analysis.getFrom(), isWindowed, ksqlConfig);
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
          new PlanNodeId("KafkaTopic_" + prefix + JoinSide.LEFT),
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
          new PlanNodeId("KafkaTopic_" + prefix + JoinSide.RIGHT),
          leaf.getSource().getDataSource(),
          leaf.getSource().getAlias(),
          isWindowed
      );
    }

    final Optional<Expression> fkExpression =
        verifyJoin(root.getInfo(), preRepartitionLeft, preRepartitionRight);

    final JoinKey joinKey = fkExpression
        .map(columnReferenceExp -> buildForeignJoinKey(root, fkExpression.get()))
        .orElseGet(() -> buildJoinKey(root));

    final PlanNode left = prepareSourceForJoin(
        root.getLeft(),
        preRepartitionLeft,
        prefix + JoinSide.LEFT,
        JoinSide.LEFT,
        root.getInfo().getLeftJoinExpression(),
        fkExpression.isPresent()
    );
    final PlanNode right = prepareSourceForJoin(
        root.getRight(),
        preRepartitionRight,
        prefix + JoinSide.RIGHT,
        JoinSide.RIGHT,
        root.getInfo().getRightJoinExpression(),
        fkExpression.isPresent()
    );

    return new JoinNode(
        new PlanNodeId(prefix + "Join"),
        root.getInfo().getType(),
        joinKey.rewriteWith(refRewriter::process),
        prefix.isEmpty(),
        left,
        right,
        root.getInfo().getWithinExpression(),
        ksqlConfig.getString(KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG)
    );
  }

  /**
   * @return the foreign key column if this is a foreign key join
   */
  private Optional<Expression> verifyJoin(
      final JoinInfo joinInfo,
      final PlanNode leftNode,
      final PlanNode rightNode
  ) {
    final JoinType joinType = joinInfo.getType();
    final Expression leftExpression = joinInfo.getLeftJoinExpression();
    final Expression rightExpression = joinInfo.getRightJoinExpression();

    if (leftNode.getNodeOutputType() == DataSourceType.KSTREAM) {
      if (rightNode.getNodeOutputType() == DataSourceType.KTABLE) {
        verifyStreamTableJoin(joinInfo, rightNode);
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

      if (joinOnNonKeyAttribute(rightExpression, rightNode, joinInfo.getRightSource())) {
        throw new KsqlException(String.format(
                "Invalid join condition:"
                    + " table-table joins require to join on the primary key of the right input"
                    + " table. Got %s = %s.",
                joinInfo.getFlippedLeftJoinExpression(),
                joinInfo.getFlippedRightJoinExpression()
           ));
      }

      if (joinOnNonKeyAttribute(leftExpression, leftNode, joinInfo.getLeftSource())) {
        // foreign key join detected

        return verifyForeignKeyJoin(joinInfo, leftNode, rightNode);
      } else {
        // primary key join detected

        final SqlType leftKeyType = Iterables.getOnlyElement(leftNode.getSchema().key()).type();
        final SqlType rightKeyType = Iterables.getOnlyElement(rightNode.getSchema().key()).type();

        verifyJoinConditionTypes(
            leftKeyType,
            rightKeyType,
            leftExpression,
            rightExpression,
            joinInfo.hasFlippedJoinCondition()
        );
      }
    }

    return Optional.empty();
  }

  private static void verifyStreamTableJoin(final JoinInfo joinInfo, final PlanNode rightNode) {
    final JoinType joinType = joinInfo.getType();
    final Expression rightExpression = joinInfo.getRightJoinExpression();

    if (joinType.equals(JoinType.OUTER)) {
      throw new KsqlException(String.format(
          "Invalid join type: "
              + "full-outer join not supported for stream-table join. Got %s %s %s.",
          joinInfo.getLeftSource().getDataSource().getName().text(),
          joinType,
          joinInfo.getRightSource().getDataSource().getName().text()
      ));
    }

    if (joinOnNonKeyAttribute(rightExpression, rightNode, joinInfo.getRightSource())) {
      throw new KsqlException(String.format(
          "Invalid join condition:"
              + " stream-table joins require to join on the table's primary key."
              + " Got %s = %s.",
          joinInfo.getFlippedLeftJoinExpression(),
          joinInfo.getFlippedRightJoinExpression()
      ));
    }
  }

  private Optional<Expression> verifyForeignKeyJoin(
      final JoinInfo joinInfo,
      final PlanNode leftNode,
      final PlanNode rightNode
  ) {
    final JoinType joinType = joinInfo.getType();
    final Expression leftExpression = joinInfo.getLeftJoinExpression();
    final Expression rightExpression = joinInfo.getRightJoinExpression();

    if (joinInfo.getType().equals(JoinType.OUTER)) {
      throw new KsqlException(String.format(
          "Invalid join type:"
              + " full-outer join not supported for foreign-key table-table join."
              + " Got %s %s %s.",
          joinInfo.getLeftSource().getDataSource().getName().text(),
          joinType,
          joinInfo.getRightSource().getDataSource().getName().text()
      ));
    }

    // after we lift this n-way join restriction, we should be able to support FK-joins
    // at any level in the join tree, even after we add right-deep/bushy join tree support,
    // because a FK-join output table has the same PK as its left input table
    if (!(leftNode instanceof DataSourceNode)
        || !(rightNode instanceof DataSourceNode)) {
      throw new KsqlException(String.format(
          "Invalid join condition:"
              + " foreign-key table-table joins are not supported as part of n-way joins."
              + " Got %s = %s.",
          joinInfo.getFlippedLeftJoinExpression(),
          joinInfo.getFlippedRightJoinExpression()
      ));
    }

    final CodeGenRunner codeGenRunner = new CodeGenRunner(
        leftNode.getSchema(),
        ksqlConfig,
        metaStore
    );

    final VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> unqualifiedRewritter =
        new VisitParentExpressionVisitor<Optional<Expression>, Context<Void>>(Optional.empty()) {
          @Override
          public Optional<Expression> visitQualifiedColumnReference(
              final QualifiedColumnReferenceExp node,
              final Context<Void> ctx
          ) {
            return Optional.of(new UnqualifiedColumnReferenceExp(node.getColumnName()));
          }
        };

    final Expression leftExpressionUnqualified =
        ExpressionTreeRewriter.rewriteWith(unqualifiedRewritter::process, leftExpression);
    final ExpressionEvaluator expressionEvaluator = codeGenRunner.buildCodeGenFromParseTree(
        leftExpressionUnqualified,
        "Left Join Expression"
    );
    final SqlType fkType = expressionEvaluator.getExpressionType();
    final SqlType rightKeyType = Iterables.getOnlyElement(rightNode.getSchema().key()).type();

    verifyJoinConditionTypes(
        fkType,
        rightKeyType,
        leftExpression,
        rightExpression,
        joinInfo.hasFlippedJoinCondition()
    );

    if (((DataSourceNode) rightNode).isWindowed()) {
      throw new KsqlException(
         "Foreign-key table-table joins are not supported on windowed tables."
      );
    }

    return Optional.of(leftExpression);
  }

  private static boolean joinOnNonKeyAttribute(
      final Expression joinExpression,
      final PlanNode node,
      final AliasedDataSource aliasedDataSource
  ) {
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
        final SourceName qualifierOrAlias = simpleJoinExpression.maybeQualifier().get();
        final SourceName qualifier;
        if (aliasedDataSource.getAlias().equals(qualifierOrAlias)) {
          qualifier = aliasedDataSource.getDataSource().getName();
        } else {
          qualifier = qualifierOrAlias;
        }
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

  private static boolean isInnerNode(final PlanNode node) {
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

  private JoinKey buildForeignJoinKey(final Join join,
                                      final Expression foreignKeyExpression) {
    final AliasedDataSource leftSource = join.getInfo().getLeftSource();
    final SourceName alias = leftSource.getAlias();
    final List<QualifiedColumnReferenceExp> leftSourceKeys =
        leftSource.getDataSource().getSchema().key().stream()
            .map(c -> new QualifiedColumnReferenceExp(alias, c.name()))
            .collect(Collectors.toList());

    final VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> aliasRewritter =
        new VisitParentExpressionVisitor<Optional<Expression>, Context<Void>>(Optional.empty()) {
          @Override
          public Optional<Expression> visitQualifiedColumnReference(
              final QualifiedColumnReferenceExp node,
              final Context<Void> ctx
          ) {
            return Optional.of(new UnqualifiedColumnReferenceExp(
                ColumnNames.generatedJoinColumnAlias(node.getQualifier(), node.getColumnName())
            ));
          }
        };

    final Expression aliasedForeignKeyExpression =
        ExpressionTreeRewriter.rewriteWith(aliasRewritter::process, foreignKeyExpression);


    return JoinKey.foreignKey(aliasedForeignKeyExpression, leftSourceKeys);
  }

  private static void verifyJoinConditionTypes(
      final SqlType leftType,
      final SqlType rightType,
      final Expression leftExpression,
      final Expression rightExpression,
      final boolean flipped
  ) {
    if (!leftType.equals(rightType)) {
      throw new KsqlException(String.format(
          "Invalid join condition: types don't match. Got %s{%s} = %s{%s}.",
          flipped ? rightExpression : leftExpression,
          flipped ? rightType : leftType,
          flipped ? leftExpression : rightExpression,
          flipped ? leftType : rightType
      ));
    }
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
      final AliasedDataSource dataSource, final boolean isWindowed, final KsqlConfig ksqlConfig) {
    return new DataSourceNode(
        new PlanNodeId("KsqlTopic"),
        dataSource.getDataSource(),
        dataSource.getAlias(),
        isWindowed
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

  /**
   * Given a source node and its side, return {@code true} if the source should be repartitioned
   * for the foreign key join.
   *
   * <p>The right hand side of the FK join must be repartitioned if it uses schema format baked by
   * Schema Registry and the primary key is a struct. In that case, there is a possibility that
   * the schema was not created by ksqlDB, therefore it won't match the generated without
   * repartitioning. See https://github.com/confluentinc/ksql/issues/8528 for more details</p>
   *
   * @param sourceNode the source node participating in a FK join
   * @param side the side of the FK join the source node participates at.
   * @return {@code true} if the source node requires repartitioning, {@code false} otherwise.
   */
  private boolean shouldRepartitionFKJoin(final PlanNode sourceNode, final JoinSide side) {
    if (!(sourceNode instanceof DataSourceNode)) {
      return false;
    }

    final DataSourceNode dataSourceNode = (DataSourceNode) sourceNode;
    final String dataSourceKeyFormat = dataSourceNode.getDataSource()
        .getKsqlTopic()
        .getKeyFormat()
        .getFormat();
    final boolean isAvro = dataSourceKeyFormat.equals(AvroFormat.NAME);
    final boolean isProtobuf = dataSourceKeyFormat.equals(ProtobufFormat.NAME);
    final boolean isJsonSR = dataSourceKeyFormat.equals(JsonSchemaFormat.NAME);
    final boolean isSREnabledFormat = isAvro || isProtobuf || isJsonSR;
    final boolean hasStructInPrimaryKey = dataSourceNode.getSchema()
        .key()
        .stream()
        .anyMatch(col -> col.type() instanceof SqlStruct);
    return side == JoinSide.RIGHT && isSREnabledFormat && hasStructInPrimaryKey;
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

  enum JoinSide {
    LEFT, RIGHT;

    @Override
    public String toString() {
      return StringUtils.capitalize(this.name().toLowerCase());
    }
  }
}
