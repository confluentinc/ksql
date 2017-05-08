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

public class ExpressionUtilTest {

    private static final KSQLParser KSQL_PARSER = new KSQLParser();
    private MetaStore metaStore;
    private Schema schema;
    private ExpressionUtil expressionUtil;

    @Before
    public void init() {
        metaStore = KSQLTestUtil.getNewMetaStore();
        schema = SchemaBuilder.struct()
                .field("TEST1.COL0", SchemaBuilder.INT64_SCHEMA)
                .field("TEST1.COL1", SchemaBuilder.STRING_SCHEMA)
                .field("TEST1.COL2", SchemaBuilder.STRING_SCHEMA)
                .field("TEST1.COL3", SchemaBuilder.FLOAT64_SCHEMA);
        expressionUtil = new ExpressionUtil();
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
        String simpleQuery = "SELECT col0+col3, col2, col3+10, col0*25, 12*4+2 FROM test1 WHERE col0 > 100;";
        Analysis analysis = analyzeQuery(simpleQuery);

        ExpressionMetadata expressionEvaluatorMetadata0 = expressionUtil.getExpressionEvaluator
            (analysis
                                                                                  .getSelectExpressions().get(0),schema);
        Assert.assertTrue(expressionEvaluatorMetadata0.getIndexes().length == 2);
        Assert.assertTrue(expressionEvaluatorMetadata0.getIndexes()[0] == 3);
        Assert.assertTrue(expressionEvaluatorMetadata0.getIndexes()[1] == 0);
        Assert.assertTrue(expressionEvaluatorMetadata0.getUdfs().length == 2);
        Object result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(new Object[]{10.0, 5l});
        Assert.assertTrue(result0 instanceof Double);
        Assert.assertTrue(((Double)result0) == 15.0);

        ExpressionMetadata expressionEvaluatorMetadata1 = expressionUtil.getExpressionEvaluator
            (analysis.getSelectExpressions().get(3),schema);
        Assert.assertTrue(expressionEvaluatorMetadata1.getIndexes().length == 1);
        Assert.assertTrue(expressionEvaluatorMetadata1.getIndexes()[0] == 0);
        Assert.assertTrue(expressionEvaluatorMetadata1.getUdfs().length == 1);
        Object result1 = expressionEvaluatorMetadata1.getExpressionEvaluator().evaluate(new Object[]{5l});
        Assert.assertTrue(result1 instanceof Long);
        Assert.assertTrue(((Long)result1) == 125l);

        ExpressionMetadata expressionEvaluatorMetadata2 = expressionUtil.getExpressionEvaluator
            (analysis.getSelectExpressions().get(4),schema);
        Assert.assertTrue(expressionEvaluatorMetadata2.getIndexes().length == 0);
        Assert.assertTrue(expressionEvaluatorMetadata2.getUdfs().length == 0);
        Object result2 = expressionEvaluatorMetadata2.getExpressionEvaluator().evaluate(new Object[]{});
        Assert.assertTrue(result2 instanceof Long);
        Assert.assertTrue(((Long)result2) == 50);
    }

    @Test
    public void testUDFExpr() throws Exception {
        String simpleQuery = "SELECT FLOOR(col3), CEIL(col3*3), ABS(col0+1.34), RANDOM()+10, ROUND(col3*2)+12 FROM test1;";
        Analysis analysis = analyzeQuery(simpleQuery);
        GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);

        ExpressionMetadata expressionEvaluator0 = expressionUtil.getExpressionEvaluator(analysis
                                                                         .getSelectExpressions().get(0),schema);
        Object argObj0 = genericRowValueTypeEnforcer.enforceFieldType(3, 1.5);
        Object result0 = expressionEvaluator0.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator0.getUdfs()
                                                                          [0], argObj0});
        Assert.assertTrue(argObj0 instanceof Double);
        Assert.assertTrue(result0 instanceof Double);
        Assert.assertTrue(((Double)result0) == 1.0);

        ExpressionMetadata expressionEvaluator1 = expressionUtil.getExpressionEvaluator(analysis
                                                                                            .getSelectExpressions().get(1),schema);
        Object argObj1 = genericRowValueTypeEnforcer.enforceFieldType(3, 1.5);
        Object result1 = expressionEvaluator1.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator1.getUdfs()
                                                                          [0], argObj1});
        Assert.assertTrue(argObj1 instanceof Double);
        Assert.assertTrue(result1 instanceof Double);
        Assert.assertTrue(((Double)result1) == 5.0);


        ExpressionMetadata expressionEvaluator2 = expressionUtil.getExpressionEvaluator(analysis
                                                                                            .getSelectExpressions().get(2),schema);
        Object argObj2 = genericRowValueTypeEnforcer.enforceFieldType(0, 15);
        Object result2 = expressionEvaluator2.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator2.getUdfs()
                                                                          [0], argObj2});
        Assert.assertTrue(argObj2 instanceof Long);
        Assert.assertTrue(result2 instanceof Double);
        Assert.assertTrue(((Double)result2) == 16.34);

        ExpressionMetadata expressionEvaluator3 = expressionUtil.getExpressionEvaluator(analysis
                                                                                            .getSelectExpressions().get(3),schema);
        Object result3 = expressionEvaluator3.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator3.getUdfs()[0]});
        Assert.assertTrue(result3 instanceof Double);
        Assert.assertTrue(((Double)result3).intValue() == 10);

        ExpressionMetadata expressionEvaluator4 = expressionUtil.getExpressionEvaluator(analysis
                                                                                            .getSelectExpressions().get(4),schema);
        Object argObj4 = genericRowValueTypeEnforcer.enforceFieldType(3, 1.5);
        Object result4 = expressionEvaluator4.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator4.getUdfs()
                                                                          [0], argObj4});
        Assert.assertTrue(argObj4 instanceof Double);
        Assert.assertTrue(result4 instanceof Long);
        Assert.assertTrue(((Long)result4) == 15);

    }

    @Test
    public void testStringUDFExpr() throws Exception {
        GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
        String simpleQuery = "SELECT LCASE(col1), UCASE(col2), TRIM(col1), CONCAT(col1,'_test'), SUBSTRING(col1, 1, 3) FROM test1;";
        Analysis analysis = analyzeQuery(simpleQuery);


        ExpressionMetadata expressionEvaluator0 = expressionUtil.getExpressionEvaluator(analysis
                                                                                            .getSelectExpressions().get(0),schema);
        Object argObj0 = genericRowValueTypeEnforcer.enforceFieldType(2, "Hello");
        Object result0 = expressionEvaluator0.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator0.getUdfs()
                                                                          [0], argObj0});
        Assert.assertTrue(result0 instanceof String);
        Assert.assertTrue(result0.equals("hello"));

        ExpressionMetadata expressionEvaluator1 = expressionUtil.getExpressionEvaluator(analysis
                                                                                            .getSelectExpressions().get(1),schema);
        Object argObj1 = genericRowValueTypeEnforcer.enforceFieldType(2, "Hello");
        Object result1 = expressionEvaluator1.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator1.getUdfs()
                                                                          [0], argObj1});
        Assert.assertTrue(result1 instanceof String);
        Assert.assertTrue(result1.equals("HELLO"));

        ExpressionMetadata expressionEvaluator2 = expressionUtil.getExpressionEvaluator(analysis
                                                                                            .getSelectExpressions().get(2),schema);
        Object argObj2 = genericRowValueTypeEnforcer.enforceFieldType(2, " Hello ");
        Object result2 = expressionEvaluator2.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator2.getUdfs()
                                                                          [0], argObj2});
        Assert.assertTrue(result2 instanceof String);
        Assert.assertTrue(result2.equals("Hello"));

        ExpressionMetadata expressionEvaluator3 = expressionUtil.getExpressionEvaluator(analysis
                                                                                            .getSelectExpressions().get(3),schema);
        Object argObj3 = genericRowValueTypeEnforcer.enforceFieldType(2, "Hello");
        Object result3 = expressionEvaluator3.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator3.getUdfs()
                                                                          [0], argObj3});
        Assert.assertTrue(result3 instanceof String);
        Assert.assertTrue(result3.equals("Hello_test"));

    }

}
