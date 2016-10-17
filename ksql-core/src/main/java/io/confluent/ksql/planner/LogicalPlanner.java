package io.confluent.ksql.planner;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.planner.plan.*;
import io.confluent.ksql.planner.types.ExpressionTypeManager;
import io.confluent.ksql.planner.types.Type;

import java.util.ArrayList;
import java.util.List;

public class LogicalPlanner
//        extends DefaultTraversalVisitor<RelationPlan, Void>
{

    Analysis analysis;

    public LogicalPlanner(Analysis analysis) {
        this.analysis = analysis;
    }

    public PlanNode buildPlan() {

        SourceNode sourceNode = buildSourceNode();
        FilterNode filterNode = buildFilterNode(sourceNode.getSchema(), sourceNode);
        ProjectNode projectNode = buildProjectNode(filterNode.getSchema(), filterNode);
        OutputNode outputNode = buildOutputNode(projectNode.getSchema(), projectNode);
        return outputNode;
    }

    private OutputNode buildOutputNode(Schema inputSchema, PlanNode sourcePlanNode) {
        DataSource intoDataSource = analysis.getInto();
        if(intoDataSource instanceof KafkaTopic) {
            KafkaTopic kafkaTopic = (KafkaTopic) intoDataSource;
            return new OutputKafkaTopicNode(new PlanNodeId(kafkaTopic.getTopicName()), sourcePlanNode, inputSchema, kafkaTopic.getTopicName());
        }
        throw new RuntimeException("INTO should be a kafka topic.");
    }

    private ProjectNode buildProjectNode(Schema inputSchema, PlanNode sourcePlanNode) {
        List<SchemaField> projectionFields = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(inputSchema);
        for(int i = 0; i < analysis.getSelectExpressions().size(); i++) {
            Expression expression = analysis.getSelectExpressions().get(i);
            String alias = analysis.getSelectExpressionAlias().get(i);

            Type expressionType = expressionTypeManager.getExpressionType(expression);

            SchemaField schemaField = new SchemaField(alias, expressionType);
            projectionFields.add(schemaField.duplicate());
            fieldNames.add(schemaField.getFieldName());

        }

        return new ProjectNode(new PlanNodeId("Project"), sourcePlanNode, new Schema(projectionFields, fieldNames), analysis.getSelectExpressions());
    }

    private FilterNode buildFilterNode(Schema inputSchema, PlanNode sourcePlanNode) {

        Expression filterExpression = analysis.getWhereExpression();
        return new FilterNode(new PlanNodeId("Filter"), sourcePlanNode, filterExpression);
    }

    private SourceNode buildSourceNode() {
        DataSource fromDataSource = analysis.getFromDataSources().get(0);
        Schema fromSchema = fromDataSource.getSchema();
        if(fromDataSource instanceof KafkaTopic) {
            KafkaTopic fromKafkaTopic = (KafkaTopic) fromDataSource;
            return new SourceKafkaTopicNode(new PlanNodeId("KafkaTopic"),fromSchema, fromKafkaTopic.getTopicName());
        }

        throw new RuntimeException("Data source is not suppoted yet.");
    }


}
