package io.confluent.kql.physical;


import io.confluent.kql.analyzer.Analysis;
import io.confluent.kql.analyzer.AnalysisContext;
import io.confluent.kql.analyzer.Analyzer;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.parser.KQLParser;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.planner.LogicalPlanner;
import io.confluent.kql.planner.plan.PlanNode;
import io.confluent.kql.structured.SchemaKStream;
import io.confluent.kql.util.KQLTestUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class PhysicalPlanBuilderTest {

    KStreamBuilder kStreamBuilder;
    KQLParser kqlParser;
    PhysicalPlanBuilder physicalPlanBuilder;
    MetaStore metaStore;

    @Before
    public void before() {
        kStreamBuilder = new KStreamBuilder();
        kqlParser = new KQLParser();
        metaStore = KQLTestUtil.getNewMetaStore();
        physicalPlanBuilder = new PhysicalPlanBuilder(kStreamBuilder);
    }

    private SchemaKStream buildPhysicalPlan(String queryStr) throws Exception {
        List<Statement> statements = kqlParser.buildAST(queryStr, metaStore);
        // Analyze the query to resolve the references and extract oeprations
        Analysis analysis = new Analysis();
        Analyzer analyzer = new Analyzer(analysis, metaStore);
        analyzer.process(statements.get(0), new AnalysisContext(null, null));
        // Build a logical plan
        PlanNode logicalPlan = new LogicalPlanner(analysis).buildPlan();
        SchemaKStream schemaKStream =  physicalPlanBuilder.buildPhysicalPlan(logicalPlan);
        return schemaKStream;
    }

    @Test
    public void testSimpleSelect() throws Exception {
        String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
        SchemaKStream schemaKStream = buildPhysicalPlan(simpleQuery);
        Assert.assertNotNull(schemaKStream);
        Assert.assertTrue(schemaKStream.getSchema().fields().size() == 3);
        Assert.assertTrue(schemaKStream.getSchema().fields().get(0).name().equalsIgnoreCase("col0"));
        Assert.assertTrue(schemaKStream.getSchema().fields().get(1).schema() == Schema.STRING_SCHEMA);
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSchema().fields().size() == 4);
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSchema().fields().get(0).name().equalsIgnoreCase("TEST1.COL0"));
    }

    @Test
    public void testSimpleLeftJoinLogicalPlan() throws Exception {
        String
                simpleQuery =
                "SELECT t1.col1, t2.col1, col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1;";
        SchemaKStream schemaKStream = buildPhysicalPlan(simpleQuery);
        Assert.assertNotNull(schemaKStream);
        Assert.assertTrue(schemaKStream.getSchema().fields().size() == 4);
        Assert.assertTrue(schemaKStream.getSchema().fields().get(0).name().equalsIgnoreCase("T1_COL1"));
        Assert.assertTrue(schemaKStream.getSchema().fields().get(1).schema() == Schema.STRING_SCHEMA);
        Assert.assertTrue(schemaKStream.getSchema().fields().get(3).name().equalsIgnoreCase("T2_COL2"));
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSourceSchemaKStreams().size() == 2);
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSchema().fields().size() == 9);
    }

    @Test
    public void testSimpleLeftJoinFilterLogicalPlan() throws Exception {
        String
                simpleQuery =
                "SELECT t1.col1, t2.col1, col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1 WHERE t1.col0 > 10 AND t2.col3 = 10.8;";
        SchemaKStream schemaKStream = buildPhysicalPlan(simpleQuery);
        Assert.assertNotNull(schemaKStream);
        Assert.assertTrue(schemaKStream.getSchema().fields().size() == 4);
        Assert.assertTrue(schemaKStream.getSchema().fields().get(1).name().equalsIgnoreCase("T2_COL1"));
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSchema().fields().size() == 9);
        Assert.assertTrue(schemaKStream.getSourceSchemaKStreams().get(0).getSourceSchemaKStreams().get(0).getSourceSchemaKStreams().size() == 2);
    }
}
