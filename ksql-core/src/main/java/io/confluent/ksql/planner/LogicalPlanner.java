package io.confluent.ksql.planner;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KafkaTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.planner.plan.*;
import io.confluent.ksql.util.ExpressionTypeManager;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.List;

public class LogicalPlanner
{

    Analysis analysis;

    public LogicalPlanner(Analysis analysis) {
        this.analysis = analysis;
    }

    public PlanNode buildPlan() {

        SourceNode sourceNode = buildSourceNode();
        PlanNode currentNode = sourceNode;
        if(analysis.getWhereExpression() != null) {
            FilterNode filterNode = buildFilterNode(currentNode.getSchema(), currentNode);
            currentNode = filterNode;
        }
        ProjectNode projectNode = buildProjectNode(currentNode.getSchema(), currentNode);
        OutputNode outputNode = buildOutputNode(projectNode.getSchema(), projectNode);
        return outputNode;
    }

    private OutputNode buildOutputNode(KSQLSchema inputSchema, PlanNode sourcePlanNode) {
        DataSource intoDataSource = analysis.getInto();
        if(intoDataSource instanceof KafkaTopic) {
            KafkaTopic kafkaTopic = (KafkaTopic) intoDataSource;
            return new OutputKafkaTopicNode(new PlanNodeId(kafkaTopic.getTopicName()), sourcePlanNode, inputSchema, kafkaTopic.getTopicName());
        }
        throw new RuntimeException("INTO should be a kafka topic.");
    }

    private ProjectNode buildProjectNode(KSQLSchema inputSchema, PlanNode sourcePlanNode) {
        List<Field> projectionFields = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(inputSchema);
        SchemaUtil schemaUtil = new SchemaUtil();
        for(int i = 0; i < analysis.getSelectExpressions().size(); i++) {
            Expression expression = analysis.getSelectExpressions().get(i);
            String alias = analysis.getSelectExpressionAlias().get(i);

            Schema.Type expressionType = expressionTypeManager.getExpressionType(expression);

            Field schemaField = new Field(alias, i, schemaUtil.getTypeSchema(expressionType));
            projectionFields.add(schemaField);
            fieldNames.add(schemaField.name());

        }

        KSQLSchema projectionSchema = new KSQLSchema(Schema.Type.STRUCT, false, null, "Project", 0, "", null, projectionFields, null, null);

        return new ProjectNode(new PlanNodeId("Project"), sourcePlanNode, projectionSchema, analysis.getSelectExpressions());
    }

    private FilterNode buildFilterNode(KSQLSchema inputSchema, PlanNode sourcePlanNode) {

        Expression filterExpression = analysis.getWhereExpression();
        return new FilterNode(new PlanNodeId("Filter"), sourcePlanNode, filterExpression);
    }

    private SourceNode buildSourceNode() {
        DataSource fromDataSource = analysis.getFromDataSources().get(0);
        KSQLSchema fromSchema = fromDataSource.getKSQLSchema();
        if(fromDataSource instanceof KafkaTopic) {
            KafkaTopic fromKafkaTopic = (KafkaTopic) fromDataSource;
            return new SourceKafkaTopicNode(new PlanNodeId("KafkaTopic"),fromSchema, fromKafkaTopic.getTopicName());
        }

        throw new RuntimeException("Data source is not suppoted yet.");
    }


}
