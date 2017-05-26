package io.confluent.ksql.util;


import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.tree.Statement;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ExpressionTypeManagerTest {

    private static final KSQLParser KSQL_PARSER = new KSQLParser();
    private MetaStore metaStore;
    private Schema schema;

    @Before
    public void init() {
        metaStore = KSQLTestUtil.getNewMetaStore();
        schema = SchemaBuilder.struct()
                .field("TEST1.COL0", SchemaBuilder.INT64_SCHEMA)
                .field("TEST1.COL1", SchemaBuilder.STRING_SCHEMA)
                .field("TEST1.COL2", SchemaBuilder.STRING_SCHEMA)
                .field("TEST1.COL3", SchemaBuilder.FLOAT64_SCHEMA);
    }

    private Analysis analyzeQuery(String queryStr) {
        List<Statement> statements = KSQL_PARSER.buildAST(queryStr, metaStore);
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
        Schema exprType0 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(0));
        Schema exprType2 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(2));
        Schema exprType3 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(3));
        Schema exprType4 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(4));
        Assert.assertTrue(exprType0 == Schema.FLOAT64_SCHEMA);
        Assert.assertTrue(exprType2 == Schema.FLOAT64_SCHEMA);
        Assert.assertTrue(exprType3 == Schema.INT64_SCHEMA);
        Assert.assertTrue(exprType4 == Schema.INT64_SCHEMA);
    }

    @Test
    public void testComparisonExpr() throws Exception {
        String simpleQuery = "SELECT col0>col3, col0*25<200, col2 = 'test' FROM test1;";
        Analysis analysis = analyzeQuery(simpleQuery);
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
        Schema exprType0 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(0));
        Schema exprType1 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(1));
        Schema exprType2 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(2));
        Assert.assertTrue(exprType0 == Schema.BOOLEAN_SCHEMA);
        Assert.assertTrue(exprType1 == Schema.BOOLEAN_SCHEMA);
        Assert.assertTrue(exprType2 == Schema.BOOLEAN_SCHEMA);
    }

    @Test
    public void testUDFExpr() throws Exception {
        String simpleQuery = "SELECT FLOOR(col3), CEIL(col3*3), ABS(col0+1.34), RANDOM()+10, ROUND(col3*2)+12 FROM test1;";
        Analysis analysis = analyzeQuery(simpleQuery);
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
        Schema exprType0 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(0));
        Schema exprType1 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(1));
        Schema exprType2 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(2));
        Schema exprType3 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(3));
        Schema exprType4 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(4));

        Assert.assertTrue(exprType0 == Schema.FLOAT64_SCHEMA);
        Assert.assertTrue(exprType1 == Schema.FLOAT64_SCHEMA);
        Assert.assertTrue(exprType2 == Schema.FLOAT64_SCHEMA);
        Assert.assertTrue(exprType3 == Schema.FLOAT64_SCHEMA);
        Assert.assertTrue(exprType4 == Schema.INT64_SCHEMA);
    }

    @Test
    public void testStringUDFExpr() throws Exception {
        String simpleQuery = "SELECT LCASE(col1), UCASE(col2), TRIM(col1), CONCAT(col1,'_test'), SUBSTRING(col1, 1, 3) FROM test1;";
        Analysis analysis = analyzeQuery(simpleQuery);
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
        Schema exprType0 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(0));
        Schema exprType1 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(1));
        Schema exprType2 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(2));
        Schema exprType3 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(3));
        Schema exprType4 = expressionTypeManager.getExpressionType(analysis.getSelectExpressions().get(4));

        Assert.assertTrue(exprType0 == Schema.STRING_SCHEMA);
        Assert.assertTrue(exprType1 == Schema.STRING_SCHEMA);
        Assert.assertTrue(exprType2 == Schema.STRING_SCHEMA);
        Assert.assertTrue(exprType3 == Schema.STRING_SCHEMA);
        Assert.assertTrue(exprType4 == Schema.STRING_SCHEMA);
    }
}
