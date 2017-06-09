package io.confluent.ksql.physical;


import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.rewrite.AggregateExpressionRewriter;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlTestUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PhysicalPlanBuilderTest {

    KStreamBuilder kStreamBuilder;
    KsqlParser ksqlParser;
    PhysicalPlanBuilder physicalPlanBuilder;
    MetaStore metaStore;

    @Before
    public void before() {
        kStreamBuilder = new KStreamBuilder();
        ksqlParser = new KsqlParser();
        metaStore = KsqlTestUtil.getNewMetaStore();
        Map configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        configMap.put("application.id", "KSQL");
        configMap.put("commit.interval.ms", 0);
        configMap.put("cache.max.bytes.buffering", 0);
        configMap.put("auto.offset.reset", "earliest");
        physicalPlanBuilder = new PhysicalPlanBuilder(kStreamBuilder, null, new KsqlConfig(configMap));
    }

    private SchemaKStream buildPhysicalPlan(String queryStr) throws Exception {
        List<Statement> statements = ksqlParser.buildAst(queryStr, metaStore);
        // Analyze the query to resolve the references and extract oeprations
//        Analysis analysis = new Analysis();
//        Analyzer analyzer = new Analyzer(analysis, metaStore);
//        analyzer.process(statements.get(0), new AnalysisContext(null, null));
//        AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
//        AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, metaStore);
//        for (Expression expression: analysis.getSelectExpressions()) {
//            aggregateAnalyzer.process(expression, new AnalysisContext(null, null));
//        }

        Analysis analysis = new Analysis();
        Analyzer analyzer = new Analyzer(analysis, metaStore);
        analyzer.process(statements.get(0), new AnalysisContext(null, null));

        AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
        AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, metaStore,
                                                                    analysis);
        AggregateExpressionRewriter aggregateExpressionRewriter = new AggregateExpressionRewriter();
        for (Expression expression: analysis.getSelectExpressions()) {
            aggregateAnalyzer.process(expression, new AnalysisContext(null, null));
            if (!aggregateAnalyzer.isHasAggregateFunction()) {
                aggregateAnalysis.getNonAggResultColumns().add(expression);
            }
            aggregateAnalysis.getFinalSelectExpressions().add(
                ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter, expression));
            aggregateAnalyzer.setHasAggregateFunction(false);
        }
        // Build a logical plan
        PlanNode logicalPlan = new LogicalPlanner(analysis, aggregateAnalysis).buildPlan();
        SchemaKStream schemaKStream =  physicalPlanBuilder.buildPhysicalPlan(logicalPlan);
        return schemaKStream;
    }

    @Test
    public void testSimpleSelect() throws Exception {
        String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
        SchemaKStream schemaKStream = buildPhysicalPlan(simpleQuery);
        Assert.assertNotNull(schemaKStream);
        Assert.assertTrue(schemaKStream.getSchema().fields().size() == 3);
        Assert.assertTrue(schemaKStream.getSchema().fields().get(0).name().equalsIgnoreCase("TEST1.COL0"));
        Assert.assertTrue(schemaKStream.getSchema().fields().get(1).schema() == Schema.STRING_SCHEMA);
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSchema().fields()
                              .size() == 6);
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSchema().fields().get(0).name().equalsIgnoreCase("TEST1.COL0"));
    }

    @Test
    public void testSimpleLeftJoinLogicalPlan() throws Exception {
        String
                simpleQuery =
                "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 "
                + "ON t1.col1 = t2.col1;";
        SchemaKStream schemaKStream = buildPhysicalPlan(simpleQuery);
        Assert.assertNotNull(schemaKStream);
        Assert.assertTrue(schemaKStream.getSchema().fields().size() == 5);
        Assert.assertTrue(schemaKStream.getSchema().fields().get(0).name().equalsIgnoreCase
            ("T1.COL1"));
        Assert.assertTrue(schemaKStream.getSchema().fields().get(1).schema() == Schema.STRING_SCHEMA);
        Assert.assertTrue(schemaKStream.getSchema().fields().get(3).name().equalsIgnoreCase
            ("T1.COL5"));
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSourceSchemaKStreams().size() == 2);
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSchema().fields()
                              .size() == 11);
    }

    @Test
    public void testSimpleLeftJoinFilterLogicalPlan() throws Exception {
        String
                simpleQuery =
                "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 "
                + "ON "
                + "t1.col1 = t2.col1 WHERE t1.col0 > 10 AND t2.col3 = 10.8;";
        SchemaKStream schemaKStream = buildPhysicalPlan(simpleQuery);
        Assert.assertNotNull(schemaKStream);
        Assert.assertTrue(schemaKStream.getSchema().fields().size() == 5);
        Assert.assertTrue(schemaKStream.getSchema().fields().get(1).name().equalsIgnoreCase
            ("T2.COL1"));
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSchema().fields()
                              .size() == 11);
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSourceSchemaKStreams().get(0).getSourceSchemaKStreams().size() == 2);
    }

    @Test
    public void testSimpleAggregate() throws Exception {
        String queryString = "SELECT col0, sum(col3), count(col3) FROM test1 window TUMBLING ( "
                             + "size 2 "
                             + "second) "
                             + "WHERE col0 > 100 GROUP BY col0;";
        SchemaKStream schemaKStream = buildPhysicalPlan(queryString);
        Assert.assertNotNull(schemaKStream);
        Assert.assertTrue(schemaKStream.getSchema().fields().size() == 3);
        Assert.assertTrue(schemaKStream.getSchema().fields().get(0).name().equalsIgnoreCase("TEST1.COL0"));
        Assert.assertTrue(schemaKStream.getSchema().fields().get(1).schema() == Schema.FLOAT64_SCHEMA);
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSchema().fields().size() == 4);
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSchema().fields().get(0).name().equalsIgnoreCase("TEST1.COL0"));
    }
}
