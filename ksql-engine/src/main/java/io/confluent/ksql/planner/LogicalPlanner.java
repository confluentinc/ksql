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
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.analyzer.Analysis.JoinInfo;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicyFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.FlatMapNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.RepartitionNode;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class LogicalPlanner {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final KsqlConfig ksqlConfig;
  private final Analysis analysis;
  private final AggregateAnalysisResult aggregateAnalysis;
  private final FunctionRegistry functionRegistry;

  public LogicalPlanner(
      final KsqlConfig ksqlConfig,
      final Analysis analysis,
      final AggregateAnalysisResult aggregateAnalysis,
      final FunctionRegistry functionRegistry
  ) {
    this.ksqlConfig = ksqlConfig;
    this.analysis = analysis;
    this.aggregateAnalysis = aggregateAnalysis;
    this.functionRegistry = functionRegistry;
  }

  public OutputNode buildPlan() {
    PlanNode currentNode = buildSourceNode();

    if (analysis.getWhereExpression().isPresent()) {
      currentNode = buildFilterNode(currentNode, analysis.getWhereExpression().get());
    }

    if (analysis.getPartitionBy().isPresent()) {
      currentNode = buildRepartitionNode(currentNode, analysis.getPartitionBy().get());
    }

    if (!analysis.getTableFunctions().isEmpty()) {
      currentNode = buildFlatMapNode(currentNode);
    }

    if (analysis.getGroupByExpressions().isEmpty()) {
      currentNode = buildProjectNode(currentNode);
    } else {
      currentNode = buildAggregateNode(currentNode);
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
        new PlanNodeId(intoDataSource.getName().name()),
        sourcePlanNode,
        inputSchema,
        timestampColumn,
        sourcePlanNode.getKeyField(),
        intoDataSource.getKsqlTopic(),
        analysis.getLimitClause(),
        intoDataSource.isCreate(),
        analysis.getSerdeOptions(),
        intoDataSource.getName()
    );
  }

  private Optional<TimestampColumn> getTimestampColumn(
      final LogicalSchema inputSchema,
      final Analysis analysis
  ) {
    final Optional<ColumnRef> timestampColumnName =
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
    final Expression groupBy = analysis.getGroupByExpressions().size() == 1
        ? analysis.getGroupByExpressions().get(0)
        : null;

    final LogicalSchema schema = buildProjectionSchema(sourcePlanNode);

    final Optional<ColumnName> keyFieldName = getSelectAliasMatching((expression, alias) ->
        expression.equals(groupBy)
            && !SchemaUtil.isFieldName(alias.name(), SchemaUtil.ROWTIME_NAME.name())
            && !SchemaUtil.isFieldName(alias.name(), SchemaUtil.ROWKEY_NAME.name()),
        sourcePlanNode);

    return new AggregateNode(
        new PlanNodeId("Aggregate"),
        sourcePlanNode,
        schema,
        keyFieldName.map(ColumnRef::withoutSource),
        analysis.getGroupByExpressions(),
        analysis.getWindowExpression(),
        aggregateAnalysis.getAggregateFunctionArguments(),
        aggregateAnalysis.getAggregateFunctions(),
        aggregateAnalysis.getRequiredColumns(),
        aggregateAnalysis.getFinalSelectExpressions(),
        aggregateAnalysis.getHavingExpression()
    );
  }

  private ProjectNode buildProjectNode(final PlanNode sourcePlanNode) {
    final ColumnRef sourceKeyFieldName = sourcePlanNode
        .getKeyField()
        .ref()
        .orElse(null);

    final LogicalSchema schema = buildProjectionSchema(sourcePlanNode);

    final Optional<ColumnName> keyFieldName = getSelectAliasMatching((expression, alias) ->
        expression instanceof ColumnReferenceExp
            && ((ColumnReferenceExp) expression).getReference().equals(sourceKeyFieldName),
        sourcePlanNode
    );

    return new ProjectNode(
        new PlanNodeId("Project"),
        sourcePlanNode,
        schema,
        keyFieldName.map(ColumnRef::withoutSource)
    );
  }

  private static FilterNode buildFilterNode(
      final PlanNode sourcePlanNode,
      final Expression filterExpression
  ) {
    return new FilterNode(new PlanNodeId("WhereFilter"), sourcePlanNode, filterExpression);
  }

  private static RepartitionNode buildRepartitionNode(
      final PlanNode sourceNode,
      final Expression partitionBy
  ) {
    if (!(partitionBy instanceof ColumnReferenceExp)) {
      return new RepartitionNode(
          new PlanNodeId("PartitionBy"),
          sourceNode,
          partitionBy,
          KeyField.none());
    }

    final ColumnRef partitionColumn = ((ColumnReferenceExp) partitionBy).getReference();
    final LogicalSchema schema = sourceNode.getSchema();

    final KeyField keyField;
    if (schema.isMetaColumn(partitionColumn.name())) {
      keyField = KeyField.none();
    } else if (schema.isKeyColumn(partitionColumn.name())) {
      keyField = sourceNode.getKeyField();
    } else {
      keyField = KeyField.of(partitionColumn);
    }

    return new RepartitionNode(
        new PlanNodeId("PartitionBy"),
        sourceNode,
        partitionBy,
        keyField);

  }

  private FlatMapNode buildFlatMapNode(final PlanNode sourcePlanNode) {
    return new FlatMapNode(new PlanNodeId("FlatMap"), sourcePlanNode, functionRegistry, analysis);
  }

  private PlanNode buildSourceNode() {

    final List<AliasedDataSource> sources = analysis.getFromDataSources();

    final Optional<JoinInfo> joinInfo = analysis.getJoin();
    if (!joinInfo.isPresent()) {
      return buildNonJoinNode(sources);
    }

    if (sources.size() != 2) {
      throw new IllegalStateException("Expected 2 sources. Got " + sources.size());
    }

    final AliasedDataSource left = sources.get(0);
    final AliasedDataSource right = sources.get(1);

    final DataSourceNode leftSourceNode = new DataSourceNode(
        new PlanNodeId("KafkaTopic_Left"),
        left.getDataSource(),
        left.getAlias(),
        analysis.getSelectExpressions()
    );

    final DataSourceNode rightSourceNode = new DataSourceNode(
        new PlanNodeId("KafkaTopic_Right"),
        right.getDataSource(),
        right.getAlias(),
        analysis.getSelectExpressions()
    );

    return new JoinNode(
        new PlanNodeId("Join"),
        analysis.getSelectExpressions(),
        joinInfo.get().getType(),
        leftSourceNode,
        rightSourceNode,
        joinInfo.get().getLeftJoinField(),
        joinInfo.get().getRightJoinField(),
        joinInfo.get().getWithinExpression()
    );
  }

  private DataSourceNode buildNonJoinNode(final List<AliasedDataSource> sources) {
    if (sources.size() != 1) {
      throw new IllegalStateException("Expected only 1 source, got: " + sources.size());
    }

    final AliasedDataSource dataSource = analysis.getFromDataSources().get(0);
    return new DataSourceNode(
        new PlanNodeId("KsqlTopic"),
        dataSource.getDataSource(),
        dataSource.getAlias(),
        analysis.getSelectExpressions()
    );
  }

  private static Optional<ColumnName> getSelectAliasMatching(
      final BiFunction<Expression, ColumnName, Boolean> matcher,
      final PlanNode sourcePlanNode
  ) {
    for (int i = 0; i < sourcePlanNode.getSelectExpressions().size(); i++) {
      final SelectExpression select = sourcePlanNode.getSelectExpressions().get(i);

      if (matcher.apply(select.getExpression(), select.getAlias())) {
        return Optional.of(select.getAlias());
      }
    }

    return Optional.empty();
  }

  private LogicalSchema buildProjectionSchema(final PlanNode sourcePlanNode) {
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        sourcePlanNode.getSchema(),
        functionRegistry
    );

    final Builder builder = LogicalSchema.builder();

    final List<Column> keyColumns = sourcePlanNode.getSchema().withoutAlias().key();

    builder.keyColumns(keyColumns);

    for (int i = 0; i < sourcePlanNode.getSelectExpressions().size(); i++) {
      final SelectExpression select = sourcePlanNode.getSelectExpressions().get(i);

      final SqlType expressionType = expressionTypeManager
          .getExpressionSqlType(select.getExpression());

      builder.valueColumn(select.getAlias(), expressionType);
    }

    return builder.build();
  }
}
