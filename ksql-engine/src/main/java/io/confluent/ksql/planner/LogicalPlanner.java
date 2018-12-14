/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.planner;

import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStdOut;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.StructuredDataSource;
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
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicyFactory;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class LogicalPlanner {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private Analysis analysis;
  private AggregateAnalysis aggregateAnalysis;
  private final FunctionRegistry functionRegistry;

  public LogicalPlanner(
      final Analysis analysis,
      final AggregateAnalysis aggregateAnalysis,
      final FunctionRegistry functionRegistry
  ) {
    this.analysis = analysis;
    this.aggregateAnalysis = aggregateAnalysis;
    this.functionRegistry = functionRegistry;
  }

  public PlanNode buildPlan() {
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
      currentNode = buildAggregateNode(currentNode.getSchema(), currentNode);
    } else {
      currentNode = buildProjectNode(currentNode.getSchema(), currentNode);
    }

    return buildOutputNode(
        currentNode.getSchema(),
        currentNode);
  }

  private OutputNode buildOutputNode(final Schema inputSchema,
                                     final PlanNode sourcePlanNode) {
    final StructuredDataSource intoDataSource = analysis.getInto();

    final Map<String, Object> intoProperties = analysis.getIntoProperties();
    final TimestampExtractionPolicy extractionPolicy = getTimestampExtractionPolicy(
        inputSchema,
        intoProperties);
    if (intoDataSource instanceof KsqlStdOut) {
      return new KsqlBareOutputNode(
          new PlanNodeId(KsqlStdOut.KSQL_STDOUT_NAME),
          sourcePlanNode,
          inputSchema,
          analysis.getLimitClause(),
          extractionPolicy
      );
    } else if (intoDataSource != null) {
      return new KsqlStructuredDataOutputNode(
          new PlanNodeId(intoDataSource.getName()),
          sourcePlanNode,
          inputSchema,
          extractionPolicy,
          sourcePlanNode.getKeyField(),
          intoDataSource.getKsqlTopic(),
          intoDataSource.getKsqlTopic().getKafkaTopicName(),
          intoProperties,
          analysis.getLimitClause(),
          analysis.isDoCreateInto()
      );

    }
    throw new RuntimeException("INTO clause is not supported in SELECT.");
  }

  private TimestampExtractionPolicy getTimestampExtractionPolicy(
      final Schema inputSchema,
      final Map<String, Object> intoProperties) {

    return TimestampExtractionPolicyFactory.create(
        inputSchema,
        (String) intoProperties.get(KsqlConstants.SINK_TIMESTAMP_COLUMN_NAME),
        (String) intoProperties.get(DdlConfig.TIMESTAMP_FORMAT_PROPERTY));
  }

  private AggregateNode buildAggregateNode(
      final Schema inputSchema,
      final PlanNode sourcePlanNode
  ) {
    SchemaBuilder aggregateSchema = SchemaBuilder.struct();
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        inputSchema,
        functionRegistry
    );
    for (int i = 0; i < analysis.getSelectExpressions().size(); i++) {
      final Expression expression = analysis.getSelectExpressions().get(i);
      final String alias = analysis.getSelectExpressionAlias().get(i);

      final Schema expressionType = expressionTypeManager.getExpressionSchema(expression);

      aggregateSchema = aggregateSchema.field(alias, expressionType);
    }

    return new AggregateNode(
        new PlanNodeId("Aggregate"),
        sourcePlanNode,
        aggregateSchema,
        analysis.getGroupByExpressions(),
        analysis.getWindowExpression(),
        aggregateAnalysis.getAggregateFunctionArguments(),
        aggregateAnalysis.getFunctionList(),
        aggregateAnalysis.getRequiredColumnsList(),
        aggregateAnalysis.getFinalSelectExpressions(),
        aggregateAnalysis.getHavingExpression()
    );
  }

  private ProjectNode buildProjectNode(final Schema inputSchema, final PlanNode sourcePlanNode) {
    SchemaBuilder projectionSchema = SchemaBuilder.struct();
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        inputSchema,
        functionRegistry
    );
    for (int i = 0; i < analysis.getSelectExpressions().size(); i++) {
      final Expression expression = analysis.getSelectExpressions().get(i);
      final String alias = analysis.getSelectExpressionAlias().get(i);

      final Schema expressionType = expressionTypeManager.getExpressionSchema(expression);

      projectionSchema = projectionSchema.field(alias, expressionType);

    }

    return new ProjectNode(
        new PlanNodeId("Project"),
        sourcePlanNode,
        projectionSchema,
        analysis.getSelectExpressions()
    );
  }

  private FilterNode buildFilterNode(final PlanNode sourcePlanNode) {

    final Expression filterExpression = analysis.getWhereExpression();
    return new FilterNode(new PlanNodeId("Filter"), sourcePlanNode, filterExpression);
  }

  private StructuredDataSourceNode buildSourceNode() {

    final Pair<StructuredDataSource, String> dataSource = analysis.getFromDataSource(0);
    final Schema fromSchema = SchemaUtil.buildSchemaWithAlias(
        dataSource.left.getSchema(),
        dataSource.right
    );

    if (dataSource.left instanceof KsqlStream || dataSource.left instanceof KsqlTable) {
      return new StructuredDataSourceNode(new PlanNodeId("KsqlTopic"), dataSource.left, fromSchema);
    }
    throw new RuntimeException("Data source is not supported yet.");
  }

}
