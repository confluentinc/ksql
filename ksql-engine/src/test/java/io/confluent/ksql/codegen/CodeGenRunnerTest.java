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
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;


public class CodeGenRunnerTest {

    private static final KsqlParser KSQL_PARSER = new KsqlParser();
    private MetaStore metaStore;
    private Schema schema;
    private CodeGenRunner codeGenRunner;
    private FunctionRegistry functionRegistry;

    final private static int INT64_INDEX1 = 0;
    final private static int STRING_INDEX1 = 1;
    final private static int STRING_INDEX2 = 2;
    final private static int FLOAT64_INDEX1 = 3;
    final private static int FLOAT64_INDEX2 = 4;
    final private static int INT32_INDEX1 = 5;
    final private static int BOOLEAN_INDEX1 = 6;
    final private static int BOOLEAN_INDEX2 = 7;
    final private static int INT64_INDEX2 = 8;
    final private static int ARRAY_INDEX1 = 9;
    final private static int ARRAY_INDEX2 = 10;
    final private static int MAP_INDEX1 = 11;
    final private static int MAP_INDEX2 = 12;

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
        assertThat(expressionEvaluatorMetadata0.getIndexes().length, equalTo(2));
        int idx0 = expressionEvaluatorMetadata0.getIndexes()[0];
        int idx1 = expressionEvaluatorMetadata0.getIndexes()[1];
        assertThat(idx0, anyOf(equalTo(cola), equalTo(colb)));
        assertThat(idx1, anyOf(equalTo(cola), equalTo(colb)));
        assertThat(idx0, not(equalTo(idx1)));
        if (idx0 == colb) {
            Object tmp = values[0];
            values[0] = values[1];
            values[1] = tmp;
        }
        assertThat(expressionEvaluatorMetadata0.getUdfs().length, equalTo(2));
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
        Assert.assertThat(evalBooleanExprEq(INT32_INDEX1, INT64_INDEX1, new Object[]{null, 12344L}), is(false));
        Assert.assertThat(evalBooleanExprEq(INT32_INDEX1, INT64_INDEX1, new Object[]{null, null}), is(false));
    }

    @Test
    public void testIsDistinctFrom() throws Exception {
        Assert.assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{12344, 12344L}), is(false));
        assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12344L}), is(true));
        assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{null, 12344L}), is(true));
        Assert.assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{null, null}), is(false));
    }

    @Test
    public void testIsNull() throws Exception {
        String simpleQuery = "SELECT col0 IS NULL FROM CODEGEN_TEST;";
        Analysis analysis = analyzeQuery(simpleQuery);

        ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0));
        assertThat(expressionEvaluatorMetadata0.getIndexes().length, equalTo(1));
        int idx0 = expressionEvaluatorMetadata0.getIndexes()[0];
        assertThat(idx0, equalTo(0));
        assertThat(expressionEvaluatorMetadata0.getUdfs().length, equalTo(1));

        Object result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(new Object[]{null});
        assertThat(result0, instanceOf(Boolean.class));
        assertThat((Boolean)result0, is(true));

        result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(new Object[]{12345L});
        assertThat(result0, instanceOf(Boolean.class));
        Assert.assertThat((Boolean)result0, is(false));
    }

    @Test
    public void testIsNotNull() throws Exception {
        String simpleQuery = "SELECT col0 IS NOT NULL FROM CODEGEN_TEST;";
        Analysis analysis = analyzeQuery(simpleQuery);

        ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0));
        assertThat(expressionEvaluatorMetadata0.getIndexes().length, equalTo(1));
        int idx0 = expressionEvaluatorMetadata0.getIndexes()[0];
        assertThat(idx0, equalTo(0));
        assertThat(expressionEvaluatorMetadata0.getUdfs().length, equalTo(1));

        Object result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(new Object[]{null});
        assertThat(result0, instanceOf(Boolean.class));
        Assert.assertThat((Boolean)result0, is(false));

        result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(new Object[]{12345L});
        assertThat(result0, instanceOf(Boolean.class));
        assertThat((Boolean)result0, is(true));
    }

    @Test
    public void testBooleanExprScalarEq() throws Exception {
        // int32
        Assert.assertThat(evalBooleanExprEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12344L}), is(false));
        assertThat(evalBooleanExprEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12345L}), is(true));
        // int64
        Assert.assertThat(evalBooleanExprEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12344}), is(false));
        assertThat(evalBooleanExprEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12345}), is(true));
        // double
        Assert.assertThat(evalBooleanExprEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12345.0, 12344.0}), is(false));
        assertThat(evalBooleanExprEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12345.0, 12345.0}), is(true));
    }

    @Test
    public void testBooleanExprBooleanEq() throws Exception {
        Assert.assertThat(evalBooleanExprEq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{false, true}), is(false));
        assertThat(evalBooleanExprEq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{true, true}), is(true));
    }

    @Test
    public void testBooleanExprStringEq() throws Exception {
        Assert.assertThat(evalBooleanExprEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(false));
        assertThat(evalBooleanExprEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(true));
    }

    @Test
    public void testBooleanExprArrayComparisonFails() throws Exception {
        Integer a1[] = new Integer[]{1, 2, 3};
        Integer a2[] = new Integer[]{1, 2, 3};
        try {
            evalBooleanExprEq(ARRAY_INDEX1, ARRAY_INDEX2, new Object[]{a1, a2});
            Assert.fail("Array comparison should throw exception");
        } catch (KsqlException e) {
            assertThat(e.getMessage(), equalTo("Cannot compare ARRAY values"));
        }
    }

    @Test
    public void testBooleanExprMapComparisonFails() throws Exception {
        HashMap<Integer, Integer> a1 = new HashMap<>();
        a1.put(1, 2);
        HashMap<Integer, Integer> a2 = new HashMap<>(a1);

        try {
            evalBooleanExprEq(MAP_INDEX1, MAP_INDEX2, new Object[]{a1, a2});
        } catch (KsqlException e) {
            assertThat(e.getMessage(), equalTo("Cannot compare MAP values"));
        }
    }

    @Test
    public void testBooleanExprScalarNeq() throws Exception {
        // int32
        assertThat(evalBooleanExprNeq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12344L}), is(true));
        Assert.assertThat(evalBooleanExprNeq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12345L}), is(false));
        // int64
        assertThat(evalBooleanExprNeq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12344}), is(true));
        Assert.assertThat(evalBooleanExprNeq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12345}), is(false));
        // double
        assertThat(evalBooleanExprNeq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12345.0, 12344.0}), is(true));
        Assert.assertThat(evalBooleanExprNeq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12345.0, 12345.0}), is(false));
    }

    @Test
    public void testBooleanExprBooleanNeq() throws Exception {
        assertThat(evalBooleanExprNeq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{false, true}), is(true));
        Assert.assertThat(evalBooleanExprNeq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{true, true}), is(false));
    }

    @Test
    public void testBooleanExprStringNeq() throws Exception {
        assertThat(evalBooleanExprNeq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(true));
        Assert.assertThat(evalBooleanExprNeq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(false));
    }

    @Test
    public void testBooleanExprScalarLessThan() throws Exception {
        // int32
        assertThat(evalBooleanExprLessThan(INT32_INDEX1, INT64_INDEX1, new Object[]{12344, 12345L}), is(true));
        Assert.assertThat(evalBooleanExprLessThan(INT32_INDEX1, INT64_INDEX1, new Object[]{12346, 12345L}), is(false));
        // int64
        assertThat(evalBooleanExprLessThan(INT64_INDEX2, INT32_INDEX1, new Object[]{12344L, 12345}), is(true));
        Assert.assertThat(evalBooleanExprLessThan(INT64_INDEX2, INT32_INDEX1, new Object[]{12346L, 12345}), is(false));
        // double
        assertThat(evalBooleanExprLessThan(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12344.0, 12345.0}), is(true));
        Assert.assertThat(evalBooleanExprLessThan(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12346.0, 12345.0}), is(false));
    }

    @Test
    public void testBooleanExprStringLessThan() throws Exception {
        assertThat(evalBooleanExprLessThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(true));
        Assert.assertThat(evalBooleanExprLessThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(false));
    }

    @Test
    public void testBooleanExprScalarLessThanEq() throws Exception {
        // int32
        assertThat(evalBooleanExprLessThanEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12345L}), is(true));
        Assert.assertThat(evalBooleanExprLessThanEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12346, 12345L}), is(false));
        // int64
        assertThat(evalBooleanExprLessThanEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12345}), is(true));
        Assert.assertThat(evalBooleanExprLessThanEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12346L, 12345}), is(false));
        // double
        assertThat(evalBooleanExprLessThanEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12344.0, 12345.0}), is(true));
        Assert.assertThat(evalBooleanExprLessThanEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12346.0, 12345.0}), is(false));
    }

    @Test
    public void testBooleanExprStringLessThanEq() throws Exception {
        assertThat(evalBooleanExprLessThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(true));
        Assert.assertThat(evalBooleanExprLessThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abb"}), is(false));
    }

    @Test
    public void testBooleanExprScalarGreaterThan() throws Exception {
        // int32
        assertThat(evalBooleanExprGreaterThan(INT32_INDEX1, INT64_INDEX1, new Object[]{12346, 12345L}), is(true));
        Assert.assertThat(evalBooleanExprGreaterThan(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12345L}), is(false));
        // int64
        assertThat(evalBooleanExprGreaterThan(INT64_INDEX2, INT32_INDEX1, new Object[]{12346L, 12345}), is(true));
        Assert.assertThat(evalBooleanExprGreaterThan(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12345}), is(false));
        // double
        assertThat(evalBooleanExprGreaterThan(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12346.0, 12345.0}), is(true));
        Assert.assertThat(evalBooleanExprGreaterThan(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12344.0, 12345.0}), is(false));
    }

    @Test
    public void testBooleanExprStringGreaterThan() throws Exception {
        assertThat(evalBooleanExprGreaterThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"def", "abc"}), is(true));
        Assert.assertThat(evalBooleanExprGreaterThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(false));
    }

    @Test
    public void testBooleanExprScalarGreaterThanEq() throws Exception {
        // int32
        assertThat(evalBooleanExprGreaterThanEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12345L}), is(true));
        Assert.assertThat(evalBooleanExprGreaterThanEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12344, 12345L}), is(false));
        // int64
        assertThat(evalBooleanExprGreaterThanEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12345}), is(true));
        Assert.assertThat(evalBooleanExprGreaterThanEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12344L, 12345}), is(false));
        // double
        assertThat(evalBooleanExprGreaterThanEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12346.0, 12345.0}), is(true));
        Assert.assertThat(evalBooleanExprGreaterThanEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12344.0, 12345.0}), is(false));
    }

    @Test
    public void testBooleanExprStringGreaterThanEq() throws Exception {
        assertThat(evalBooleanExprGreaterThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"def", "abc"}), is(true));
        Assert.assertThat(evalBooleanExprGreaterThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(false));
    }

    @Test
    public void testArithmaticExpr() throws Exception {
        String simpleQuery = "SELECT col0+col3, col2, col3+10, col0*25, 12*4+2 FROM codegen_test WHERE col0 > 100;";
        Analysis analysis = analyzeQuery(simpleQuery);

        ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0));
        assertThat(expressionEvaluatorMetadata0.getIndexes().length, equalTo(2));
        assertThat(expressionEvaluatorMetadata0.getIndexes()[0], equalTo(3));
        assertThat(expressionEvaluatorMetadata0.getIndexes()[1], equalTo(0));
        assertThat(expressionEvaluatorMetadata0.getUdfs().length, equalTo(2));
        Object result0 = expressionEvaluatorMetadata0.getExpressionEvaluator().evaluate(new Object[]{10.0, 5l});
        assertThat(result0, instanceOf(Double.class));
        assertThat(((Double)result0), equalTo(15.0));

        ExpressionMetadata expressionEvaluatorMetadata1 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(3));
        assertThat(expressionEvaluatorMetadata1.getIndexes().length, equalTo(1));
        assertThat(expressionEvaluatorMetadata1.getIndexes()[0], equalTo(0));
        assertThat(expressionEvaluatorMetadata1.getUdfs().length, equalTo(1));
        Object result1 = expressionEvaluatorMetadata1.getExpressionEvaluator().evaluate(new Object[]{5l});
        assertThat(result1, instanceOf(Long.class));
        assertThat(((Long)result1), equalTo(125l));

        ExpressionMetadata expressionEvaluatorMetadata2 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(4));
        assertThat(expressionEvaluatorMetadata2.getIndexes().length, equalTo(0));
        assertThat(expressionEvaluatorMetadata2.getUdfs().length, equalTo(0));
        Object result2 = expressionEvaluatorMetadata2.getExpressionEvaluator().evaluate(new Object[]{});
        assertThat(result2, instanceOf(Long.class));
        assertThat(((Long)result2), equalTo(50L));
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
        assertThat(argObj0, instanceOf(Double.class));
        assertThat(result0, instanceOf(Double.class));
        assertThat(((Double)result0), equalTo(1.0));

        ExpressionMetadata expressionEvaluator1 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(1));
        Object argObj1 = genericRowValueTypeEnforcer.enforceFieldType(3, 1.5);
        Object result1 = evaluateU1DF(expressionEvaluator1, argObj1);
        assertThat(argObj1, instanceOf(Double.class));
        assertThat(result1, instanceOf(Double.class));
        assertThat(((Double)result1), equalTo(5.0));


        ExpressionMetadata expressionEvaluator2 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(2));
        Object argObj2 = genericRowValueTypeEnforcer.enforceFieldType(0, 15);
        Object result2 = evaluateU1DF(expressionEvaluator2, argObj2);
        assertThat(argObj2, instanceOf(Long.class));
        assertThat(result2, instanceOf(Double.class));
        assertThat(((Double)result2), equalTo(16.34));

        ExpressionMetadata expressionEvaluator3 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(3));
        Object result3 = expressionEvaluator3.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator3.getUdfs()[0]});
        assertThat(result3, instanceOf(Double.class));
        assertThat(((Double)result3).intValue(), equalTo(10));

        ExpressionMetadata expressionEvaluator4 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(4));
        Object argObj4 = genericRowValueTypeEnforcer.enforceFieldType(3, 1.5);
        Object result4 = evaluateU1DF(expressionEvaluator4, argObj4);
        assertThat(argObj4, instanceOf(Double.class));
        assertThat(result4, instanceOf(Long.class));
        assertThat(((Long)result4), equalTo(15L));

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
        assertThat(result0, instanceOf(String.class));
        assertThat(result0, equalTo("hello"));

        ExpressionMetadata expressionEvaluator1 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(1));
        Object argObj1 = genericRowValueTypeEnforcer.enforceFieldType(2, "Hello");
        Object result1 = expressionEvaluator1.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator1.getUdfs()
                                                                          [0], argObj1});
        assertThat(result1, instanceOf(String.class));
        assertThat(result1, equalTo("HELLO"));

        ExpressionMetadata expressionEvaluator2 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(2));
        Object argObj2 = genericRowValueTypeEnforcer.enforceFieldType(2, " Hello ");
        Object result2 = expressionEvaluator2.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator2.getUdfs()
                                                                          [0], argObj2});
        assertThat(result2, instanceOf(String.class));
        assertThat(result2, equalTo("Hello"));

        ExpressionMetadata expressionEvaluator3 = codeGenRunner.buildCodeGenFromParseTree(analysis
                                                                                            .getSelectExpressions().get(3));
        Object argObj3 = genericRowValueTypeEnforcer.enforceFieldType(2, "Hello");
        Object result3 = expressionEvaluator3.getExpressionEvaluator().evaluate(new
                                                             Object[]{expressionEvaluator3.getUdfs()
                                                                          [0], argObj3});
        assertThat(result3, instanceOf(String.class));
        assertThat(result3, equalTo("Hello_test"));

    }

}
