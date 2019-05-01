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
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import io.confluent.ksql.util.ExpressionTypeManager;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicyFactory;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class LogicalPlanner {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final Analysis analysis;
  private final AggregateAnalysisResult aggregateAnalysis;
  private final FunctionRegistry functionRegistry;

  public LogicalPlanner(
      final Analysis analysis,
      final AggregateAnalysisResult aggregateAnalysis,
      final FunctionRegistry functionRegistry
  ) {
    this.analysis = analysis;
    this.aggregateAnalysis = aggregateAnalysis;
    this.functionRegistry = functionRegistry;
  }

  public OutputNode buildPlan() {
    PlanNode currentNode;
    if (analysis.getJoin() != null) {
      currentNode = analysis.getJoin();
    } else {
      currentNode = buildSourceNode();
    }
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
    final Schema inputSchema = sourcePlanNode.getSchema();
    final Map<String, Object> intoProperties = analysis.getIntoProperties();
    final TimestampExtractionPolicy extractionPolicy =
        getTimestampExtractionPolicy(inputSchema, intoProperties);

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

    final Optional<Field> partitionByField = Optional
        .ofNullable(intoProperties.get(DdlConfig.PARTITION_BY_PROPERTY))
        .map(Object::toString)
        .map(keyName -> SchemaUtil.getFieldByName(inputSchema, keyName)
            .orElseThrow(() -> new KsqlException(
                "Column " + keyName + " does not exist in the result schema. "
                    + "Error in Partition By clause.")
            ));

    final KeyField keyField = partitionByField
        .map(Field::name)
        .map(newKeyField -> sourcePlanNode.getKeyField().withName(newKeyField))
        .orElse(sourcePlanNode.getKeyField());

    return new KsqlStructuredDataOutputNode(
        new PlanNodeId(intoDataSource.getName()),
        sourcePlanNode,
        inputSchema,
        extractionPolicy,
        keyField,
        intoDataSource.getKsqlTopic(),
        intoDataSource.getKsqlTopic().getKafkaTopicName(),
        intoProperties,
        analysis.getLimitClause(),
        intoDataSource.isCreate()
    );
  }

  private static TimestampExtractionPolicy getTimestampExtractionPolicy(
      final Schema inputSchema,
      final Map<String, Object> intoProperties) {

    return TimestampExtractionPolicyFactory.create(
        inputSchema,
        (String) intoProperties.get(KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME),
        (String) intoProperties.get(DdlConfig.TIMESTAMP_FORMAT_PROPERTY));
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
    SchemaBuilder aggregateSchema = SchemaBuilder.struct();
    for (int i = 0; i < analysis.getSelectExpressions().size(); i++) {
      final Expression expression = analysis.getSelectExpressions().get(i);
      final String alias = analysis.getSelectExpressionAlias().get(i);

      final Schema expressionType = expressionTypeManager.getExpressionSchema(expression);

      aggregateSchema = aggregateSchema.field(alias, expressionType);

      if (expression.equals(groupBy)) {
        keyField = Optional.of(alias);
      }
    }

    return new AggregateNode(
        new PlanNodeId("Aggregate"),
        sourcePlanNode,
        aggregateSchema,
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

    return new ProjectNode(
        new PlanNodeId("Project"),
        sourcePlanNode,
        projectionSchema.build(),
        keyFieldName,
        analysis.getSelectExpressions()
    );
  }

  private FilterNode buildFilterNode(final PlanNode sourcePlanNode) {

    final Expression filterExpression = analysis.getWhereExpression();
    return new FilterNode(new PlanNodeId("Filter"), sourcePlanNode, filterExpression);
  }

  private StructuredDataSourceNode buildSourceNode() {

    final Pair<DataSource<?>, String> dataSource = analysis.getFromDataSource(0);
    if (!(dataSource.left instanceof KsqlStream) && !(dataSource.left instanceof KsqlTable)) {
      throw new RuntimeException("Data source is not supported yet.");
    }

    return new StructuredDataSourceNode(
        new PlanNodeId("KsqlTopic"),
        dataSource.left,
        dataSource.right
    );
  }
}
