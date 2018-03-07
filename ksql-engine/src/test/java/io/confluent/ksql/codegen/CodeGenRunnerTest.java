/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.codegen;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.MetaStoreFixture;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;


public class CodeGenRunnerTest {

    private static final KsqlParser KSQL_PARSER = new KsqlParser();
    private MetaStore metaStore;
    private Schema schema;
    private CodeGenRunner codeGenRunner;
    private FunctionRegistry functionRegistry;

    @Before
    public void init() {
        metaStore = MetaStoreFixture.getNewMetaStore();
        functionRegistry = new FunctionRegistry();
        schema = SchemaBuilder.struct()
                .field("CODEGEN_TEST.COL0", SchemaBuilder.INT64_SCHEMA)
                .field("CODEGEN_TEST.COL1", SchemaBuilder.STRING_SCHEMA)
                .field("CODEGEN_TEST.COL2", SchemaBuilder.STRING_SCHEMA)
                .field("CODEGEN_TEST.COL3", SchemaBuilder.FLOAT64_SCHEMA)
                .field("CODEGEN_TEST.COL4", SchemaBuilder.FLOAT64_SCHEMA)
                .field("CODEGEN_TEST.COL5", SchemaBuilder.INT32_SCHEMA)
                .field("CODEGEN_TEST.COL6", SchemaBuilder.BOOLEAN_SCHEMA)
                .field("CODEGEN_TEST.COL7", SchemaBuilder.BOOLEAN_SCHEMA)
                .field("CODEGEN_TEST.COL8", SchemaBuilder.INT64_SCHEMA)
                .field("CODEGEN_TEST.COL9", SchemaBuilder.array(SchemaBuilder.INT32_SCHEMA))
                .field("CODEGEN_TEST.COL10", SchemaBuilder.array(SchemaBuilder.INT32_SCHEMA))
                .field("CODEGEN_TEST.COL11",
                    SchemaBuilder.map(SchemaBuilder.INT32_SCHEMA, SchemaBuilder.INT32_SCHEMA))
                .field("CODEGEN_TEST.COL12",
                    SchemaBuilder.map(SchemaBuilder.INT32_SCHEMA, SchemaBuilder.INT32_SCHEMA));
        Schema metaStoreSchema = SchemaBuilder.struct()
            .field("COL0", SchemaBuilder.INT64_SCHEMA)
            .field("COL1", SchemaBuilder.STRING_SCHEMA)
            .field("COL2", SchemaBuilder.STRING_SCHEMA)
            .field("COL3", SchemaBuilder.FLOAT64_SCHEMA)
            .field("COL4", SchemaBuilder.FLOAT64_SCHEMA)
            .field("COL5", SchemaBuilder.INT32_SCHEMA)
            .field("COL6", SchemaBuilder.BOOLEAN_SCHEMA)
            .field("COL7", SchemaBuilder.BOOLEAN_SCHEMA)
            .field("COL8", SchemaBuilder.INT64_SCHEMA)
            .field("COL9", SchemaBuilder.array(SchemaBuilder.INT32_SCHEMA))
            .field("COL10", SchemaBuilder.array(SchemaBuilder.INT32_SCHEMA))
            .field("COL11",
                SchemaBuilder.map(SchemaBuilder.INT32_SCHEMA, SchemaBuilder.INT32_SCHEMA))
            .field("COL12",
                SchemaBuilder.map(SchemaBuilder.INT32_SCHEMA, SchemaBuilder.INT32_SCHEMA));
        KsqlTopic ksqlTopic = new KsqlTopic(
            "CODEGEN_TEST",
            "codegen_test",
            new KsqlJsonTopicSerDe());
        KsqlStream ksqlStream = new KsqlStream(
            "sqlexpression",
            "CODEGEN_TEST", metaStoreSchema,
            metaStoreSchema.field("COL0"),
            null,
            ksqlTopic);
        metaStore.putTopic(ksqlTopic);
        metaStore.putSource(ksqlStream);
        codeGenRunner = new CodeGenRunner(schema, functionRegistry);
    }

    private Analysis analyzeQuery(String queryStr) {
        List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
        // Analyze the query to resolve the references and extract oeprations
        Analysis analysis = new Analysis();
        Analyzer analyzer = new Analyzer(queryStr, analysis, metaStore);
        analyzer.process(statements.get(0), new AnalysisContext(null));
        return analysis;
    }

    private boolean evalBooleanExpr(String queryFormat, int cola, int colb, Object values[])
            throws Exception {
        String simpleQuery = String.format(queryFormat, cola, colb);
        Analysis analysis = analyzeQuery(simpleQuery);

        ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0));
        Assert.assertTrue(expressionEvaluatorMetadata0.getIndexes().length == 2);
        int idx0 = expressionEvaluatorMetadata0.getIndexes()[0];
        int idx1 = expressionEvaluatorMetadata0.getIndexes()[1];
        Assert.assertThat(idx0, anyOf(equalTo(cola), equalTo(colb)));
        Assert.assertThat(idx1, anyOf(equalTo(cola), equalTo(colb)));
        Assert.assertNotEquals(idx0, idx1);
        if (idx0 == colb) {
            Object tmp = values[0];
            values[0] = values[1];
            values[1] = tmp;
        }
        Assert.assertEquals(expressionEvaluatorMetadata0.getUdfs().length, 2);
        Object result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(values);
        assertThat(result0, instanceOf(Boolean.class));
        return (Boolean)result0;
    }

    private boolean evalBooleanExprEq(int cola, int colb, Object values[]) throws Exception {
        return evalBooleanExpr("SELECT col%d = col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprNeq(int cola, int colb, Object values[]) throws Exception {
        return evalBooleanExpr("SELECT col%d != col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprIsDistinctFrom(int cola, int colb, Object values[]) throws Exception {
        return evalBooleanExpr("SELECT col%d IS DISTINCT FROM col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprLessThan(int cola, int colb, Object values[]) throws Exception {
        return evalBooleanExpr("SELECT col%d < col%d FROM CODEGEN_TEST;", cola, colb, values);
    }


    private boolean evalBooleanExprLessThanEq(int cola, int colb, Object values[]) throws Exception {
        return evalBooleanExpr("SELECT col%d <= col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprGreaterThan(int cola, int colb, Object values[]) throws Exception {
        return evalBooleanExpr("SELECT col%d > col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprGreaterThanEq(int cola, int colb, Object values[]) throws Exception {
        return evalBooleanExpr("SELECT col%d >= col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    @Test
    public void testNullEquals() throws Exception {
        Assert.assertFalse(evalBooleanExprEq(5, 0, new Object[]{null, 12344L}));
        Assert.assertFalse(evalBooleanExprEq(5, 0, new Object[]{null, null}));
    }

    @Test
    public void testIsDistinctFrom() throws Exception {
        Assert.assertFalse(evalBooleanExprIsDistinctFrom(5, 0, new Object[]{12344, 12344L}));
        Assert.assertTrue(evalBooleanExprIsDistinctFrom(5, 0, new Object[]{12345, 12344L}));
        Assert.assertTrue(evalBooleanExprIsDistinctFrom(5, 0, new Object[]{null, 12344L}));
        Assert.assertFalse(evalBooleanExprIsDistinctFrom(5, 0, new Object[]{null, null}));
    }

    @Test
    public void testIsNull() throws Exception {
        String simpleQuery = "SELECT col0 IS NULL FROM CODEGEN_TEST;";
        Analysis analysis = analyzeQuery(simpleQuery);

        ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0));
        Assert.assertTrue(expressionEvaluatorMetadata0.getIndexes().length == 1);
        int idx0 = expressionEvaluatorMetadata0.getIndexes()[0];
        Assert.assertEquals(idx0, 0);
        Assert.assertEquals(expressionEvaluatorMetadata0.getUdfs().length, 1);

        Object result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(new Object[]{null});
        assertThat(result0, instanceOf(Boolean.class));
        Assert.assertTrue((Boolean)result0);

        result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(new Object[]{12345L});
        assertThat(result0, instanceOf(Boolean.class));
        Assert.assertFalse((Boolean)result0);
    }

    @Test
    public void testIsNotNull() throws Exception {
        String simpleQuery = "SELECT col0 IS NOT NULL FROM CODEGEN_TEST;";
        Analysis analysis = analyzeQuery(simpleQuery);

        ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0));
        Assert.assertTrue(expressionEvaluatorMetadata0.getIndexes().length == 1);
        int idx0 = expressionEvaluatorMetadata0.getIndexes()[0];
        Assert.assertEquals(idx0, 0);
        Assert.assertEquals(expressionEvaluatorMetadata0.getUdfs().length, 1);

        Object result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(new Object[]{null});
        assertThat(result0, instanceOf(Boolean.class));
        Assert.assertFalse((Boolean)result0);

        result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(new Object[]{12345L});
        assertThat(result0, instanceOf(Boolean.class));
        Assert.assertTrue((Boolean)result0);
    }

    @Test
    public void testBooleanExprScalarEq() throws Exception {
        // int32
        Assert.assertFalse(evalBooleanExprEq(5, 0, new Object[]{12345, 12344L}));
        Assert.assertTrue(evalBooleanExprEq(5, 0, new Object[]{12345, 12345L}));
        // int64
        Assert.assertFalse(evalBooleanExprEq(8, 5, new Object[]{12345L, 12344}));
        Assert.assertTrue(evalBooleanExprEq(8, 5, new Object[]{12345L, 12345}));
        // double
        Assert.assertFalse(evalBooleanExprEq(4, 3, new Object[]{12345.0, 12344.0}));
        Assert.assertTrue(evalBooleanExprEq(4, 3, new Object[]{12345.0, 12345.0}));
    }

    @Test
    public void testBooleanExprBooleanEq() throws Exception {
        Assert.assertFalse(evalBooleanExprEq(7, 6, new Object[]{false, true}));
        Assert.assertTrue(evalBooleanExprEq(7, 6, new Object[]{true, true}));
    }

    @Test
    public void testBooleanExprStringEq() throws Exception {
        Assert.assertFalse(evalBooleanExprEq(1, 2, new Object[]{"abc", "def"}));
        Assert.assertTrue(evalBooleanExprEq(1, 2, new Object[]{"abc", "abc"}));
    }

    @Test
    public void testBooleanExprArrayEq() throws Exception {
        Integer a1[] = new Integer[]{1, 2, 3};
        Integer a2[] = new Integer[]{1, 2, 3};
        Integer b[] = new Integer[]{4, 5, 6};

        Assert.assertFalse(evalBooleanExprEq(9, 10, new Object[]{a1, b}));
        Assert.assertTrue(evalBooleanExprEq(9, 10, new Object[]{a1, a2}));
    }

    @Test
    public void testBooleanExprMapEq() throws Exception {
        HashMap<Integer, Integer> a1 = new HashMap<>();
        a1.put(1, 2);
        HashMap<Integer, Integer> a2 = new HashMap<>(a1);
        HashMap<Integer, Integer> b = new HashMap<>();
        b.put(5, 6);

        Assert.assertFalse(evalBooleanExprEq(11, 12, new Object[]{a1, b}));
        Assert.assertTrue(evalBooleanExprEq(11, 12, new Object[]{a1, a2}));
    }

    @Test
    public void testBooleanExprScalarNeq() throws Exception {
        // int32
        Assert.assertTrue(evalBooleanExprNeq(5, 0, new Object[]{12345, 12344L}));
        Assert.assertFalse(evalBooleanExprNeq(5, 0, new Object[]{12345, 12345L}));
        // int64
        Assert.assertTrue(evalBooleanExprNeq(8, 5, new Object[]{12345L, 12344}));
        Assert.assertFalse(evalBooleanExprNeq(8, 5, new Object[]{12345L, 12345}));
        // double
        Assert.assertTrue(evalBooleanExprNeq(4, 3, new Object[]{12345.0, 12344.0}));
        Assert.assertFalse(evalBooleanExprNeq(4, 3, new Object[]{12345.0, 12345.0}));
    }

    @Test
    public void testBooleanExprBooleanNeq() throws Exception {
        Assert.assertTrue(evalBooleanExprNeq(7, 6, new Object[]{false, true}));
        Assert.assertFalse(evalBooleanExprNeq(7, 6, new Object[]{true, true}));
    }

    @Test
    public void testBooleanExprStringNeq() throws Exception {
        Assert.assertTrue(evalBooleanExprNeq(1, 2, new Object[]{"abc", "def"}));
        Assert.assertFalse(evalBooleanExprNeq(1, 2, new Object[]{"abc", "abc"}));
    }

    @Test
    public void testBooleanExprArrayNeq() throws Exception {
        Integer a1[] = new Integer[]{1, 2, 3};
        Integer a2[] = new Integer[]{1, 2, 3};
        Integer b[] = new Integer[]{4, 5, 6};

        Assert.assertTrue(evalBooleanExprNeq(9, 10, new Object[]{a1, b}));
        Assert.assertFalse(evalBooleanExprNeq(9, 10, new Object[]{a1, a2}));
    }

    @Test
    public void testBooleanExprMapNeq() throws Exception {
        HashMap<Integer, Integer> a1 = new HashMap<>();
        a1.put(1, 2);
        HashMap<Integer, Integer> a2 = new HashMap<>(a1);
        HashMap<Integer, Integer> b = new HashMap<>();
        b.put(5, 6);

        Assert.assertTrue(evalBooleanExprNeq(11, 12, new Object[]{a1, b}));
        Assert.assertFalse(evalBooleanExprNeq(11, 12, new Object[]{a1, a2}));
    }

    @Test
    public void testBooleanExprScalarLessThan() throws Exception {
        // int32
        Assert.assertTrue(evalBooleanExprLessThan(5, 0, new Object[]{12344, 12345L}));
        Assert.assertFalse(evalBooleanExprLessThan(5, 0, new Object[]{12346, 12345L}));
        // int64
        Assert.assertTrue(evalBooleanExprLessThan(8, 5, new Object[]{12344L, 12345}));
        Assert.assertFalse(evalBooleanExprLessThan(8, 5, new Object[]{12346L, 12345}));
        // double
        Assert.assertTrue(evalBooleanExprLessThan(4, 3, new Object[]{12344.0, 12345.0}));
        Assert.assertFalse(evalBooleanExprLessThan(4, 3, new Object[]{12346.0, 12345.0}));
    }

    @Test
    public void testBooleanExprStringLessThan() throws Exception {
        Assert.assertTrue(evalBooleanExprLessThan(1, 2, new Object[]{"abc", "def"}));
        Assert.assertFalse(evalBooleanExprLessThan(1, 2, new Object[]{"abc", "abc"}));
    }

    @Test
    public void testBooleanExprScalarLessThanEq() throws Exception {
        // int32
        Assert.assertTrue(evalBooleanExprLessThanEq(5, 0, new Object[]{12345, 12345L}));
        Assert.assertFalse(evalBooleanExprLessThanEq(5, 0, new Object[]{12346, 12345L}));
        // int64
        Assert.assertTrue(evalBooleanExprLessThanEq(8, 5, new Object[]{12345L, 12345}));
        Assert.assertFalse(evalBooleanExprLessThanEq(8, 5, new Object[]{12346L, 12345}));
        // double
        Assert.assertTrue(evalBooleanExprLessThanEq(4, 3, new Object[]{12344.0, 12345.0}));
        Assert.assertFalse(evalBooleanExprLessThanEq(4, 3, new Object[]{12346.0, 12345.0}));
    }

    @Test
    public void testBooleanExprStringLessThanEq() throws Exception {
        Assert.assertTrue(evalBooleanExprLessThanEq(1, 2, new Object[]{"abc", "abc"}));
        Assert.assertFalse(evalBooleanExprLessThanEq(1, 2, new Object[]{"abc", "abb"}));
    }

    @Test
    public void testBooleanExprScalarGreaterThan() throws Exception {
        // int32
        Assert.assertTrue(evalBooleanExprGreaterThan(5, 0, new Object[]{12346, 12345L}));
        Assert.assertFalse(evalBooleanExprGreaterThan(5, 0, new Object[]{12345, 12345L}));
        // int64
        Assert.assertTrue(evalBooleanExprGreaterThan(8, 5, new Object[]{12346L, 12345}));
        Assert.assertFalse(evalBooleanExprGreaterThan(8, 5, new Object[]{12345L, 12345}));
        // double
        Assert.assertTrue(evalBooleanExprGreaterThan(4, 3, new Object[]{12346.0, 12345.0}));
        Assert.assertFalse(evalBooleanExprGreaterThan(4, 3, new Object[]{12344.0, 12345.0}));
    }

    @Test
    public void testBooleanExprStringGreaterThan() throws Exception {
        Assert.assertTrue(evalBooleanExprGreaterThan(1, 2, new Object[]{"def", "abc"}));
        Assert.assertFalse(evalBooleanExprGreaterThan(1, 2, new Object[]{"abc", "abc"}));
    }

    @Test
    public void testBooleanExprScalarGreaterThanEq() throws Exception {
        // int32
        Assert.assertTrue(evalBooleanExprGreaterThanEq(5, 0, new Object[]{12345, 12345L}));
        Assert.assertFalse(evalBooleanExprGreaterThanEq(5, 0, new Object[]{12344, 12345L}));
        // int64
        Assert.assertTrue(evalBooleanExprGreaterThanEq(8, 5, new Object[]{12345L, 12345}));
        Assert.assertFalse(evalBooleanExprGreaterThanEq(8, 5, new Object[]{12344L, 12345}));
        // double
        Assert.assertTrue(evalBooleanExprGreaterThanEq(4, 3, new Object[]{12346.0, 12345.0}));
        Assert.assertFalse(evalBooleanExprGreaterThanEq(4, 3, new Object[]{12344.0, 12345.0}));
    }

    @Test
    public void testBooleanExprStringGreaterThanEq() throws Exception {
        Assert.assertTrue(evalBooleanExprGreaterThanEq(1, 2, new Object[]{"def", "abc"}));
        Assert.assertFalse(evalBooleanExprGreaterThanEq(1, 2, new Object[]{"abc", "def"}));
    }

    @Test
    public void testArithmaticExpr() throws Exception {
        String simpleQuery = "SELECT col0+col3, col2, col3+10, col0*25, 12*4+2 FROM codegen_test WHERE col0 > 100;";
        Analysis analysis = analyzeQuery(simpleQuery);

        ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0));
        Assert.assertTrue(expressionEvaluatorMetadata0.getIndexes().length == 2);
        Assert.assertTrue(expressionEvaluatorMetadata0.getIndexes()[0] == 3);
        Assert.assertTrue(expressionEvaluatorMetadata0.getIndexes()[1] == 0);
        Assert.assertTrue(expressionEvaluatorMetadata0.getUdfs().length == 2);
        Object result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(new Object[]{10.0, 5l});
        Assert.assertTrue(result0 instanceof Double);
        Assert.assertTrue(((Double)result0) == 15.0);

        ExpressionMetadata expressionEvaluatorMetadata1 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(3));
        Assert.assertTrue(expressionEvaluatorMetadata1.getIndexes().length == 1);
        Assert.assertTrue(expressionEvaluatorMetadata1.getIndexes()[0] == 0);
        Assert.assertTrue(expressionEvaluatorMetadata1.getUdfs().length == 1);
        Object result1 = expressionEvaluatorMetadata1.getExpressionEvaluator().evaluate(new Object[]{5l});
        Assert.assertTrue(result1 instanceof Long);
        Assert.assertTrue(((Long)result1) == 125l);

        ExpressionMetadata expressionEvaluatorMetadata2 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(4));
        Assert.assertTrue(expressionEvaluatorMetadata2.getIndexes().length == 0);
        Assert.assertTrue(expressionEvaluatorMetadata2.getUdfs().length == 0);
        Object result2 = expressionEvaluatorMetadata2.getExpressionEvaluator().evaluate(new Object[]{});
        Assert.assertTrue(result2 instanceof Long);
        Assert.assertTrue(((Long)result2) == 50);
    }

    private Object evaluateU1DF(ExpressionMetadata expressionEvaluator, Object arg) throws Exception {
        Object args[] = new Object[2];
        int argsIdx = 0;
        for (int i : expressionEvaluator.getIndexes()) {
            if (i == -1) {
                args[argsIdx] = expressionEvaluator.getUdfs()[argsIdx];
            } else {
                args[argsIdx] = arg;
            }
            argsIdx++;
        }
        return expressionEvaluator.getExpressionEvaluator().evaluate(args);
    }

    @Test
    public void testU1DFExpr() throws Exception {
        String simpleQuery = "SELECT FLOOR(col3), CEIL(col3*3), ABS(col0+1.34), RANDOM()+10, ROUND(col3*2)+12 FROM codegen_test;";
        Analysis analysis = analyzeQuery(simpleQuery);
        GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);

        ExpressionMetadata expressionEvaluator0 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                         .getSelectExpressions()
                                                                                              .get(0));
        Object argObj0 = genericRowValueTypeEnforcer.enforceFieldType(3, 1.5);
        Object result0 = expressionEvaluator0.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator0.getUdfs()
                                                                          [0], argObj0});
        Assert.assertTrue(argObj0 instanceof Double);
        Assert.assertTrue(result0 instanceof Double);
        Assert.assertTrue(((Double)result0) == 1.0);

        ExpressionMetadata expressionEvaluator1 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(1));
        Object argObj1 = genericRowValueTypeEnforcer.enforceFieldType(3, 1.5);
        Object result1 = evaluateU1DF(expressionEvaluator1, argObj1);
        Assert.assertTrue(argObj1 instanceof Double);
        Assert.assertTrue(result1 instanceof Double);
        Assert.assertTrue(((Double)result1) == 5.0);


        ExpressionMetadata expressionEvaluator2 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(2));
        Object argObj2 = genericRowValueTypeEnforcer.enforceFieldType(0, 15);
        Object result2 = evaluateU1DF(expressionEvaluator2, argObj2);
        Assert.assertTrue(argObj2 instanceof Long);
        Assert.assertTrue(result2 instanceof Double);
        Assert.assertTrue(((Double)result2) == 16.34);

        ExpressionMetadata expressionEvaluator3 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(3));
        Object result3 = expressionEvaluator3.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator3.getUdfs()[0]});
        Assert.assertTrue(result3 instanceof Double);
        Assert.assertTrue(((Double)result3).intValue() == 10);

        ExpressionMetadata expressionEvaluator4 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(4));
        Object argObj4 = genericRowValueTypeEnforcer.enforceFieldType(3, 1.5);
        Object result4 = evaluateU1DF(expressionEvaluator4, argObj4);
        Assert.assertTrue(argObj4 instanceof Double);
        Assert.assertTrue(result4 instanceof Long);
        Assert.assertTrue(((Long)result4) == 15);

    }

    @Test
    public void testStringUDFExpr() throws Exception {
        GenericRowValueTypeEnforcer genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
        String simpleQuery = "SELECT LCASE(col1), UCASE(col2), TRIM(col1), CONCAT(col1,'_test'), SUBSTRING(col1, 1, 3) FROM codegen_test;";
        Analysis analysis = analyzeQuery(simpleQuery);


        ExpressionMetadata expressionEvaluator0 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(0));
        Object argObj0 = genericRowValueTypeEnforcer.enforceFieldType(2, "Hello");
        Object result0 = expressionEvaluator0.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator0.getUdfs()
                                                                          [0], argObj0});
        Assert.assertTrue(result0 instanceof String);
        Assert.assertTrue(result0.equals("hello"));

        ExpressionMetadata expressionEvaluator1 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(1));
        Object argObj1 = genericRowValueTypeEnforcer.enforceFieldType(2, "Hello");
        Object result1 = expressionEvaluator1.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator1.getUdfs()
                                                                          [0], argObj1});
        Assert.assertTrue(result1 instanceof String);
        Assert.assertTrue(result1.equals("HELLO"));

        ExpressionMetadata expressionEvaluator2 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(2));
        Object argObj2 = genericRowValueTypeEnforcer.enforceFieldType(2, " Hello ");
        Object result2 = expressionEvaluator2.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator2.getUdfs()
                                                                          [0], argObj2});
        Assert.assertTrue(result2 instanceof String);
        Assert.assertTrue(result2.equals("Hello"));

        ExpressionMetadata expressionEvaluator3 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(3));
        Object argObj3 = genericRowValueTypeEnforcer.enforceFieldType(2, "Hello");
        Object result3 = expressionEvaluator3.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator3.getUdfs()
                                                                          [0], argObj3});
        Assert.assertTrue(result3 instanceof String);
        Assert.assertTrue(result3.equals("Hello_test"));

    }

}
