package io.confluent.ksql.planner;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.metastore.KQLStream;
import io.confluent.ksql.metastore.KQLTable;
import io.confluent.ksql.metastore.KQL_STDOUT;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.planner.plan.*;
import io.confluent.ksql.util.ExpressionTypeManager;
import io.confluent.ksql.util.SchemaUtil;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;

public class LogicalPlanner {

  Analysis analysis;

  public LogicalPlanner(Analysis analysis) {
    this.analysis = analysis;
  }

  public PlanNode buildPlan() {

    PlanNode currentNode;
    if (analysis.getJoin() != null) {
      currentNode = analysis.getJoin();
    } else {
      SourceNode sourceNode = buildSourceNode();
      currentNode = sourceNode;
    }
    if (analysis.getWhereExpression() != null) {
      FilterNode filterNode = buildFilterNode(currentNode.getSchema(), currentNode);
      currentNode = filterNode;
    }
    ProjectNode projectNode = buildProjectNode(currentNode.getSchema(), currentNode);
    OutputNode outputNode = buildOutputNode(projectNode.getSchema(), projectNode);
    return outputNode;
  }

  private OutputNode buildOutputNode(Schema inputSchema, PlanNode sourcePlanNode) {
    StructuredDataSource intoDataSource = analysis.getInto();

    if (intoDataSource instanceof KQL_STDOUT) {
      return new KQLConsoleOutputNode(new PlanNodeId(KQL_STDOUT.KQL_STDOUT_NAME), sourcePlanNode,
                                      inputSchema);
    } else if (intoDataSource instanceof StructuredDataSource) {
      StructuredDataSource intoStructuredDataSource = (StructuredDataSource) intoDataSource;

      return new KQLStructuredDataOutputNode(new PlanNodeId(intoDataSource.getName()), sourcePlanNode,
                                             inputSchema, intoStructuredDataSource.getKQLTopic(),
                                             intoStructuredDataSource.getKQLTopic()
                                                 .getTopicName());
//      KQLTopic kafkaTopic = (KQLTopic) intoDataSource;
//      return new OutputKafkaTopicNode(new PlanNodeId(kafkaTopic.getTopicName()), sourcePlanNode,
//                                      inputSchema, kafkaTopic.getTopicName());
//      return null;
    }

    throw new RuntimeException("INTO caluse is not supported in SELECT.");
  }

  private ProjectNode buildProjectNode(Schema inputSchema, PlanNode sourcePlanNode) {
    List<Field> projectionFields = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();

    SchemaBuilder projectionSchema = SchemaBuilder.struct();
    ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(inputSchema);
    for (int i = 0; i < analysis.getSelectExpressions().size(); i++) {
      Expression expression = analysis.getSelectExpressions().get(i);
      String alias = analysis.getSelectExpressionAlias().get(i);

      Schema.Type expressionType = expressionTypeManager.getExpressionType(expression);

      projectionSchema = projectionSchema.field(alias, SchemaUtil.getTypeSchema(expressionType));

    }

    return new ProjectNode(new PlanNodeId("Project"), sourcePlanNode, projectionSchema,
                           analysis.getSelectExpressions());
  }

  private FilterNode buildFilterNode(Schema inputSchema, PlanNode sourcePlanNode) {

    Expression filterExpression = analysis.getWhereExpression();
    return new FilterNode(new PlanNodeId("Filter"), sourcePlanNode, filterExpression);
  }

  private SourceNode buildSourceNode() {

    StructuredDataSource fromDataSource = analysis.getFromDataSources().get(0).getLeft();
    String alias = analysis.getFromDataSources().get(0).getRight();
    Schema fromSchema = SchemaUtil.buildSchemaWithAlias(fromDataSource.getSchema(), alias);
//    if (fromDataSource instanceof KQLTopic) {
//      KQLTopic fromKafkaTopic = (KQLTopic) fromDataSource;
//      return new SourceKafkaTopicNode(new PlanNodeId("KQLTopic"), fromSchema,
//                                      fromDataSource.getKeyField(), fromKafkaTopic.getTopicName(),
//                                      alias, fromKafkaTopic.getDataSourceType(),
//                                      ((KQLTopic) fromDataSource).getKqlTopicSerDe());
//    }

    if (fromDataSource instanceof KQLStream) {
      KQLStream fromStream = (KQLStream) fromDataSource;
      return new StructuredDataSourceNode(new PlanNodeId("KQLTopic"), fromSchema,
                                      fromDataSource.getKeyField(), fromStream.getKQLTopic().getTopicName(),
                                      alias, fromStream.getDataSourceType(),
                                      fromStream);
    } else if (fromDataSource instanceof KQLTable) {
      KQLTable fromTable = (KQLTable) fromDataSource;
      return new StructuredDataSourceNode(new PlanNodeId("KQLTopic"), fromSchema,
                                      fromDataSource.getKeyField(), fromTable.getKQLTopic().getTopicName(),
                                      alias, fromTable.getDataSourceType(),
                                      fromTable);
    }

    throw new RuntimeException("Data source is not suppoted yet.");
  }

}
