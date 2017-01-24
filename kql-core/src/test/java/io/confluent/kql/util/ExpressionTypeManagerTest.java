package io.confluent.kql.util;


import io.confluent.kql.analyzer.Analysis;
import io.confluent.kql.analyzer.AnalysisContext;
import io.confluent.kql.analyzer.Analyzer;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.parser.KQLParser;
import io.confluent.kql.parser.tree.Statement;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ExpressionTypeManagerTest {

    private static final KQLParser kqlParser = new KQLParser();
    private MetaStore metaStore;
    private Schema schema;

    @Before
    public void init() {
        metaStore = KQLTestUtil.getNewMetaStore();
        schema = SchemaBuilder.struct()
                .field("TEST1.COL0", SchemaBuilder.INT64_SCHEMA)
                .field("TEST1.COL1", SchemaBuilder.STRING_SCHEMA)
                .field("TEST1.COL2", SchemaBuilder.STRING_SCHEMA)
                .field("TEST1.COL3", SchemaBuilder.FLOAT64_SCHEMA);
    }

    private Analysis analyzeQuery(String queryStr) {
        List<Statement> statements = kqlParser.buildAST(queryStr, metaStore);
        // Analyze the query to resolve the references and extract oeprations
        Analysis analysis = new Analysis();
        Analyzer analyzer = new Analyzer(analysis, metaStore);
        analyzer.process(statements.get(0), new AnalysisContext(null, null));
        return analysis;
    }

    @Test
    public void testArithmaticExpr() throws Exception {
        String simpleQuery = "SELECT col0+col3, col2, col3+10, col0+10, col0*25 FROM test1 WHERE col0 > 100;";
        Analysis analysis = analyzeQuery(simpleQuery);
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
        Schema.Type exprType0 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(0));
        Schema.Type exprType2 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(2));
        Schema.Type exprType3 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(3));
        Schema.Type exprType4 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(4));
        Assert.assertTrue(exprType0 == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType2 == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType3 == Schema.Type.INT64);
        Assert.assertTrue(exprType4 == Schema.Type.INT64);
    }

    @Test
    public void testComparisonExpr() throws Exception {
        String simpleQuery = "SELECT col0>col3, col0*25<200, col2 = 'test' FROM test1;";
        Analysis analysis = analyzeQuery(simpleQuery);
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
        Schema.Type exprType0 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(0));
        Schema.Type exprType1 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(1));
        Schema.Type exprType2 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(2));
        Assert.assertTrue(exprType0 == Schema.Type.BOOLEAN);
        Assert.assertTrue(exprType1 == Schema.Type.BOOLEAN);
        Assert.assertTrue(exprType2 == Schema.Type.BOOLEAN);
    }

    @Test
    public void testUDFExpr() throws Exception {
        String simpleQuery = "SELECT FLOOR(col3), CEIL(col3*3), ABS(col0+1.34), RANDOM()+10, ROUND(col3*2)+12 FROM test1;";
        Analysis analysis = analyzeQuery(simpleQuery);
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
        Schema.Type exprType0 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(0));
        Schema.Type exprType1 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(1));
        Schema.Type exprType2 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(2));
        Schema.Type exprType3 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(3));
        Schema.Type exprType4 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(4));

        Assert.assertTrue(exprType0 == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType1 == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType2 == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType3 == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType4 == Schema.Type.INT64);
    }

    @Test
    public void testStringUDFExpr() throws Exception {
        String simpleQuery = "SELECT LCASE(col1), UCASE(col2), TRIM(col1), CONCAT(col1,'_test'), SUBSTRING(col1, 1, 3) FROM test1;";
        Analysis analysis = analyzeQuery(simpleQuery);
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
        Schema.Type exprType0 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(0));
        Schema.Type exprType1 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(1));
        Schema.Type exprType2 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(2));
        Schema.Type exprType3 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(3));
        Schema.Type exprType4 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(4));

        Assert.assertTrue(exprType0 == Schema.Type.STRING);
        Assert.assertTrue(exprType1 == Schema.Type.STRING);
        Assert.assertTrue(exprType2 == Schema.Type.STRING);
        Assert.assertTrue(exprType3 == Schema.Type.STRING);
        Assert.assertTrue(exprType4 == Schema.Type.STRING);
    }
}
