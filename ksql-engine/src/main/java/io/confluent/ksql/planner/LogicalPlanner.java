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
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.ExpressionTypeManager;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicyFactory;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

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

    if (analysis.getWhereExpression() != null) {
      currentNode = buildFilterNode(currentNode);
    }

    if (!analysis.getGroupByExpressions().isEmpty()) {
      currentNode = buildAggregateNode(currentNode);
    } else {
      currentNode = buildProjectNode(currentNode);
    }

    return buildOutputNode(currentNode);
  }

  private OutputNode buildOutputNode(final PlanNode sourcePlanNode) {
    final LogicalSchema inputSchema = sourcePlanNode.getSchema();
    final TimestampExtractionPolicy extractionPolicy =
        getTimestampExtractionPolicy(inputSchema, analysis);

    if (!analysis.getInto().isPresent()) {
      return new KsqlBareOutputNode(
          new PlanNodeId("KSQL_STDOUT_NAME"),
          sourcePlanNode,
          inputSchema,
          analysis.getLimitClause(),
          extractionPolicy
      );
    }

    final Into intoDataSource = analysis.getInto().get();

    final Optional<String> partitionByField = analysis.getPartitionBy();

    partitionByField.ifPresent(keyName ->
        inputSchema.findValueField(keyName)
            .orElseThrow(() -> new KsqlException(
                "Column " + keyName + " does not exist in the result schema. "
                    + "Error in Partition By clause.")
            ));

    final KeyField keyField = buildOutputKeyField(sourcePlanNode);

    return new KsqlStructuredDataOutputNode(
        new PlanNodeId(intoDataSource.getName()),
        sourcePlanNode,
        inputSchema,
        extractionPolicy,
        keyField,
        intoDataSource.getKsqlTopic(),
        partitionByField,
        analysis.getLimitClause(),
        intoDataSource.isCreate(),
        analysis.getSerdeOptions()
    );
  }

  private KeyField buildOutputKeyField(
      final PlanNode sourcePlanNode
  ) {
    final KeyField sourceKeyField = sourcePlanNode.getKeyField();

    final Optional<String> partitionByField = analysis.getPartitionBy();
    if (!partitionByField.isPresent()) {
      return sourceKeyField;
    }

    final String partitionBy = partitionByField.get();
    final LogicalSchema schema = sourcePlanNode.getSchema();

    if (schema.isMetaField(partitionBy)) {
      return sourceKeyField.withName(Optional.empty());
    }

    if (schema.isKeyField(partitionBy)) {
      return sourceKeyField;
    }

    return sourceKeyField.withName(partitionBy);
  }

  private TimestampExtractionPolicy getTimestampExtractionPolicy(
      final LogicalSchema inputSchema,
      final Analysis analysis
  ) {
    return TimestampExtractionPolicyFactory.create(
        ksqlConfig,
        inputSchema,
        analysis.getProperties().getTimestampColumnName(),
        analysis.getProperties().getTimestampFormat());
  }

  private AggregateNode buildAggregateNode(final PlanNode sourcePlanNode) {
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        sourcePlanNode.getSchema(),
        functionRegistry
    );

    final Expression groupBy = analysis.getGroupByExpressions().size() == 1
        ? analysis.getGroupByExpressions().get(0)
        : null;

    Optional<String> keyField = Optional.empty();
    final SchemaBuilder aggregateSchema = SchemaBuilder.struct();
    for (int i = 0; i < analysis.getSelectExpressions().size(); i++) {
      final Expression expression = analysis.getSelectExpressions().get(i);
      final String alias = analysis.getSelectExpressionAlias().get(i);

      final Schema expressionType = expressionTypeManager.getExpressionSchema(expression);

      aggregateSchema.field(alias, expressionType);

      if (expression.equals(groupBy)
          && !SchemaUtil.isFieldName(alias, SchemaUtil.ROWTIME_NAME)
          && !SchemaUtil.isFieldName(alias, SchemaUtil.ROWKEY_NAME)) {
        keyField = Optional.of(alias);
      }
    }

    final ConnectSchema keySchema = sourcePlanNode.getSchema().isAliased()
        ? sourcePlanNode.getSchema().withoutAlias().keySchema()
        : sourcePlanNode.getSchema().keySchema();

    final LogicalSchema schema = LogicalSchema.of(
        keySchema,
        aggregateSchema.build()
    );

    return new AggregateNode(
        new PlanNodeId("Aggregate"),
        sourcePlanNode,
        schema,
        keyField,
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
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        sourcePlanNode.getSchema(),
        functionRegistry
    );

    final String sourceKeyFieldName = sourcePlanNode
        .getKeyField()
        .name()
        .orElse(null);

    Optional<String> keyFieldName = Optional.empty();
    final SchemaBuilder projectionSchema = SchemaBuilder.struct();
    for (int i = 0; i < analysis.getSelectExpressions().size(); i++) {
      final Expression expression = analysis.getSelectExpressions().get(i);
      final String alias = analysis.getSelectExpressionAlias().get(i);

      final Schema expressionType = expressionTypeManager.getExpressionSchema(expression);

      projectionSchema.field(alias, expressionType);

      if (expression instanceof DereferenceExpression
          && expression.toString().equals(sourceKeyFieldName)) {
        keyFieldName = Optional.of(alias);
      }
    }

    final ConnectSchema keySchema = sourcePlanNode.getSchema().isAliased()
        ? sourcePlanNode.getSchema().withoutAlias().keySchema()
        : sourcePlanNode.getSchema().keySchema();

    final LogicalSchema schema = LogicalSchema.of(keySchema, projectionSchema.build());

    return new ProjectNode(
        new PlanNodeId("Project"),
        sourcePlanNode,
        schema,
        keyFieldName,
        analysis.getSelectExpressions()
    );
  }

  private FilterNode buildFilterNode(final PlanNode sourcePlanNode) {

    final Expression filterExpression = analysis.getWhereExpression();
    return new FilterNode(new PlanNodeId("Filter"), sourcePlanNode, filterExpression);
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
        left.getAlias()
    );

    final DataSourceNode rightSourceNode = new DataSourceNode(
        new PlanNodeId("KafkaTopic_Right"),
        right.getDataSource(),
        right.getAlias()
    );

    return new JoinNode(
        new PlanNodeId("Join"),
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
        dataSource.getAlias()
    );
  }
}
