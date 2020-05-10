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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.udf.AsValue;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
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
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.RepartitionNode;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
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
import java.util.stream.IntStream;
import java.util.stream.Stream;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class LogicalPlanner {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final KsqlConfig ksqlConfig;
  private final RewrittenAnalysis analysis;
  private final FunctionRegistry functionRegistry;
  private final AggregateAnalyzer aggregateAnalyzer;
  private final ColumnReferenceRewriter refRewriter;

  public LogicalPlanner(
      final KsqlConfig ksqlConfig,
      final ImmutableAnalysis analysis,
      final FunctionRegistry functionRegistry
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.refRewriter =
        new ColumnReferenceRewriter(analysis.getFromSourceSchemas(false).isJoin());
    this.analysis = new RewrittenAnalysis(analysis, refRewriter::process);
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.aggregateAnalyzer = new AggregateAnalyzer(functionRegistry);
  }

  public OutputNode buildPlan() {
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
      currentNode = buildUserProjectNode(currentNode);
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
        sourcePlanNode.getKeyField(),
        intoDataSource.getKsqlTopic(),
        analysis.getLimitClause(),
        intoDataSource.isCreate(),
        getSerdeOptions(sourcePlanNode, intoDataSource),
        intoDataSource.getName()
    );
  }

  private Set<SerdeOption> getSerdeOptions(
      final PlanNode sourcePlanNode,
      final Into intoDataSource
  ) {
    final List<ColumnName> columnNames = sourcePlanNode.getSchema().value().stream()
        .map(Column::name)
        .collect(Collectors.toList());

    final Format valueFormat = intoDataSource.getKsqlTopic()
        .getValueFormat()
        .getFormat();

    return SerdeOptions.buildForCreateAsStatement(
        columnNames,
        valueFormat,
        analysis.getProperties().getWrapSingleValues(),
        intoDataSource.getDefaultSerdeOptions()
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

  private AggregateNode buildAggregateNode(final PlanNode sourcePlanNode) {
    final GroupBy groupBy = analysis.getGroupBy()
        .orElseThrow(IllegalStateException::new);

    final List<Expression> groupByExps = groupBy
        .getGroupingExpressions();

    final List<SelectExpression> projectionExpressions = buildSelectExpressions(
        sourcePlanNode,
        analysis.getSelectItems()
    );

    final LogicalSchema schema =
        buildAggregateSchema(sourcePlanNode, groupBy, projectionExpressions);

    final Expression groupBySingle = groupByExps.size() == 1
        ? groupByExps.get(0)
        : null;

    final Optional<ColumnName> keyFieldName = getSelectAliasMatching(
        (expression, alias) ->
            expression.equals(groupBySingle)
                && !SystemColumns.isSystemColumn(alias)
                && !schema.isKeyColumn(alias),
        projectionExpressions);

    final RewrittenAggregateAnalysis aggregateAnalysis = new RewrittenAggregateAnalysis(
        aggregateAnalyzer.analyze(analysis, projectionExpressions),
        refRewriter::process
    );

    if (analysis.getHavingExpression().isPresent()) {
      final FilterTypeValidator validator = new FilterTypeValidator(
          sourcePlanNode.getSchema(),
          functionRegistry,
          FilterType.HAVING);

      validator.validateFilterExpression(analysis.getHavingExpression().get());
    }

    return new AggregateNode(
        new PlanNodeId("Aggregate"),
        sourcePlanNode,
        schema,
        keyFieldName,
        groupBy,
        functionRegistry,
        analysis,
        aggregateAnalysis,
        projectionExpressions
    );
  }

  private ProjectNode buildUserProjectNode(final PlanNode parentNode) {
    final List<SelectExpression> projection =
        buildSelectExpressions(parentNode, analysis.getSelectItems());

    final LogicalSchema schema = buildProjectionSchema(parentNode.getSchema(), projection);

    final boolean persistent = analysis.getInto().isPresent();
    if (persistent) {
      // Persistent queries have key columns asa key columns - so final projection can exclude them:
      final Set<SelectExpression> keySelects = parentNode.getSchema().key().stream()
          .map(Column::name)
          .map(name -> resolveProjectionAlias(name, projection))
          .collect(Collectors.toSet());

      projection.removeIf(keySelects::contains);
    }

    final LogicalSchema nodeSchema;
    if (persistent) {
      nodeSchema = schema;
    } else {
      // Transient queries return key columns in the value, so the projection includes them, and
      // the schema needs to include them too:
      final Builder builder = LogicalSchema.builder();

      schema.columns()
          .forEach(builder::valueColumn);

      nodeSchema = builder.build();
    }

    return new FinalProjectNode(
        new PlanNodeId("Project"),
        parentNode,
        projection,
        nodeSchema,
        analysis.getSelectItems()
    );
  }

  private static SelectExpression resolveProjectionAlias(
      final ColumnName name,
      final List<SelectExpression> projection
  ) {
    final ColumnName alias = projection.stream()
        .filter(se -> se.getExpression() instanceof ColumnReferenceExp)
        .filter(se -> ((ColumnReferenceExp) se.getExpression()).getColumnName().equals(name))
        .map(SelectExpression::getAlias)
        .findFirst()
        .orElse(name);

    return SelectExpression.of(alias, new UnqualifiedColumnReferenceExp(name));
  }

  private static ProjectNode buildInternalProjectNode(
      final PlanNode parent,
      final String id,
      final SourceName sourceAlias
  ) {
    final List<SelectExpression> projection = selectWithPrependAlias(
        sourceAlias,
        parent.getSchema()
    );

    final LogicalSchema schema = buildJoinSourceSchema(sourceAlias, parent.getSchema());

    return new ProjectNode(
        new PlanNodeId(id),
        parent,
        projection,
        schema,
        true
    );
  }

  private static List<SelectExpression> buildSelectExpressions(
      final PlanNode parentNode,
      final List<SelectItem> selectItems
  ) {
    return IntStream.range(0, selectItems.size())
        .boxed()
        .flatMap(idx -> resolveSelectItem(idx, selectItems, parentNode))
        .collect(Collectors.toList());
  }

  private static Stream<SelectExpression> resolveSelectItem(
      final int idx,
      final List<SelectItem> selectItems,
      final PlanNode parentNode
  ) {
    final SelectItem selectItem = selectItems.get(idx);

    if (selectItem instanceof SingleColumn) {
      final SingleColumn column = (SingleColumn) selectItem;
      final Expression expression = parentNode.resolveSelect(idx, column.getExpression());
      final ColumnName alias = column.getAlias()
          .orElseThrow(() -> new IllegalStateException("Alias should be present by this point"));

      return Stream.of(SelectExpression.of(alias, expression));
    }

    if (selectItem instanceof AllColumns) {
      final AllColumns allColumns = (AllColumns) selectItem;

      final Stream<ColumnName> columns = parentNode
          .resolveSelectStar(allColumns.getSource());

      return columns
          .map(name -> SelectExpression.of(name, new UnqualifiedColumnReferenceExp(
              allColumns.getLocation(),
              name
          )));
    }

    throw new IllegalArgumentException(
        "Unsupported SelectItem type: " + selectItem.getClass().getName());
  }

  private FilterNode buildFilterNode(
      final PlanNode sourcePlanNode,
      final Expression filterExpression
  ) {
    final FilterTypeValidator validator = new FilterTypeValidator(
        sourcePlanNode.getSchema(),
        functionRegistry,
        FilterType.WHERE);

    validator.validateFilterExpression(filterExpression);

    return new FilterNode(new PlanNodeId("WhereFilter"), sourcePlanNode, filterExpression);
  }

  private RepartitionNode buildUserRepartitionNode(
      final PlanNode currentNode,
      final PartitionBy partitionBy
  ) {
    return buildRepartitionNode(
        "PartitionBy",
        currentNode,
        partitionBy.getExpression(),
        refRewriter::process,
        false
    );
  }

  private RepartitionNode buildInternalRepartitionNode(
      final PlanNode source,
      final String side,
      final Expression joinExpression,
      final BiFunction<Expression, Context<Void>, Optional<Expression>> plugin
  ) {
    return buildRepartitionNode(
        side + "SourceKeyed",
        source,
        joinExpression,
        plugin,
        true
    );
  }

  private RepartitionNode buildRepartitionNode(
      final String planId,
      final PlanNode sourceNode,
      final Expression partitionBy,
      final BiFunction<Expression, Context<Void>, Optional<Expression>> plugin,
      final boolean internal
  ) {
    final Expression rewrittenPartitionBy =
        ExpressionTreeRewriter.rewriteWith(plugin, partitionBy);

    final KeyField keyField;

    if (!(rewrittenPartitionBy instanceof UnqualifiedColumnReferenceExp)) {
      keyField = KeyField.none();
    } else {
      final ColumnName columnName = ((UnqualifiedColumnReferenceExp) rewrittenPartitionBy)
          .getColumnName();

      final LogicalSchema sourceSchema = sourceNode.getSchema();

      final Column proposedKey = sourceSchema
          .findColumn(columnName)
          .orElseThrow(IllegalStateException::new);

      switch (proposedKey.namespace()) {
        case KEY:
          keyField = sourceNode.getKeyField();
          break;
        case VALUE:
          keyField = KeyField.of(columnName);
          break;
        default:
          keyField = KeyField.none();
          break;
      }
    }

    final LogicalSchema schema =
        buildRepartitionedSchema(sourceNode, rewrittenPartitionBy);

    return new RepartitionNode(
        new PlanNodeId(planId),
        sourceNode,
        schema,
        partitionBy,
        rewrittenPartitionBy,
        keyField,
        internal
    );
  }

  private FlatMapNode buildFlatMapNode(final PlanNode sourcePlanNode) {
    return new FlatMapNode(new PlanNodeId("FlatMap"), sourcePlanNode, functionRegistry, analysis);
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

  private static LogicalSchema buildJoinSourceSchema(
      final SourceName alias,
      final LogicalSchema parentSchema
  ) {
    final Builder builder = LogicalSchema.builder();

    parentSchema.columns()
        .forEach(c -> {
          final ColumnName aliasedName = ColumnNames.generatedJoinColumnAlias(alias, c.name());

          if (c.namespace() == Namespace.KEY) {
            builder.keyColumn(aliasedName, c.type());
          } else {
            builder.valueColumn(aliasedName, c.type());
          }
        });

    return builder.build();
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
    final JoinInfo info = join.getInfo();

    final List<QualifiedColumnReferenceExp> viableKeyColumns = join.viableKeyColumns();
    if (viableKeyColumns.isEmpty()) {
      return JoinKey.syntheticColumn(info.getLeftJoinExpression(), info.getRightJoinExpression());
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

  private static Optional<ColumnName> getSelectAliasMatching(
      final BiFunction<Expression, ColumnName, Boolean> matcher,
      final List<SelectExpression> projection
  ) {
    for (final SelectExpression select : projection) {
      if (matcher.apply(select.getExpression(), select.getAlias())) {
        return Optional.of(select.getAlias());
      }
    }

    return Optional.empty();
  }

  private LogicalSchema buildProjectionSchema(
      final LogicalSchema parentSchema,
      final List<SelectExpression> projection
  ) {
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        parentSchema,
        functionRegistry
    );

    final Builder builder = LogicalSchema.builder();

    final ImmutableMap.Builder<ColumnName, SqlType> keys = ImmutableMap.builder();

    for (final SelectExpression select : projection) {
      final Expression expression = select.getExpression();

      final SqlType expressionType = expressionTypeManager
          .getExpressionSqlType(expression);

      final boolean keyColumn = expression instanceof ColumnReferenceExp
          && parentSchema.isKeyColumn(((ColumnReferenceExp) expression).getColumnName());

      if (keyColumn) {
        builder.keyColumn(select.getAlias(), expressionType);
        keys.put(select.getAlias(), expressionType);
      } else {
        builder.valueColumn(select.getAlias(), expressionType);
      }
    }

    return builder.build();
  }

  private LogicalSchema buildAggregateSchema(
      final PlanNode sourcePlanNode,
      final GroupBy groupBy,
      final List<SelectExpression> projectionExpressions
  ) {
    final LogicalSchema sourceSchema = sourcePlanNode.getSchema();

    final LogicalSchema projectionSchema = buildProjectionSchema(
        sourceSchema
            .withPseudoAndKeyColsInValue(analysis.getWindowExpression().isPresent()),
        projectionExpressions
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

    final Optional<ColumnName> keyName;
    final SqlType keyType;
    final Set<ColumnName> keyColumnNames;

    if (groupByExps.size() != 1) {
      keyType = SqlTypes.STRING;

      keyName = Optional.of(ColumnNames.nextKsqlColAlias(
          sourceSchema,
          LogicalSchema.builder()
              .valueColumns(projectionSchema.value())
              .build()
      ));

      keyColumnNames = groupBy.getGroupingExpressions().stream()
          .map(selectResolver)
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toSet());

    } else {
      final ExpressionTypeManager typeManager =
          new ExpressionTypeManager(sourceSchema, functionRegistry);

      final Expression expression = groupByExps.get(0);

      keyName = selectResolver.apply(expression);
      keyType = typeManager.getExpressionSqlType(expression);
      keyColumnNames = keyName.map(ImmutableSet::of).orElse(ImmutableSet.of());
    }

    final List<Column> valueColumns = projectionSchema.value().stream()
        .filter(col -> !keyColumnNames.contains(col.name()))
        .collect(Collectors.toList());

    final Builder builder = LogicalSchema.builder();

    keyName
        .ifPresent(name -> builder.keyColumn(name, keyType));

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
        functionRegistry
    );
  }

  private static boolean exactlyMatchesKeyColumns(
      final Expression expression,
      final LogicalSchema schema
  ) {
    if (schema.key().size() != 1) {
      // Currently only support single key column:
      return false;
    }

    if (!(expression instanceof ColumnReferenceExp)) {
      // Anything not a column ref can't be a match:
      return false;
    }

    final ColumnName columnName = ((ColumnReferenceExp) expression).getColumnName();

    final Namespace ns = schema
        .findColumn(columnName)
        .map(Column::namespace)
        .orElse(Namespace.VALUE);

    return ns == Namespace.KEY;
  }

  private static List<SelectExpression> selectWithPrependAlias(
      final SourceName alias,
      final LogicalSchema schema
  ) {
    return schema.value().stream()
        .map(c -> SelectExpression.of(
            ColumnNames.generatedJoinColumnAlias(alias, c.name()),
            new UnqualifiedColumnReferenceExp(c.name()))
        ).collect(Collectors.toList());
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
            ColumnNames.generatedJoinColumnAlias(node.getQualifier(), node.getColumnName())
        ));
      } else {
        return Optional.of(new UnqualifiedColumnReferenceExp(node.getColumnName()));
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
