/*
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.UdfLoaderUtil;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


@SuppressWarnings("SameParameterValue")
public class CodeGenRunnerTest {

    private static final KsqlParser KSQL_PARSER = new KsqlParser();
    private static final int INT64_INDEX1 = 0;
    private static final int STRING_INDEX1 = 1;
    private static final int STRING_INDEX2 = 2;
    private static final int FLOAT64_INDEX1 = 3;
    private static final int FLOAT64_INDEX2 = 4;
    private static final int INT32_INDEX1 = 5;
    private static final int BOOLEAN_INDEX1 = 6;
    private static final int BOOLEAN_INDEX2 = 7;
    private static final int INT64_INDEX2 = 8;
    private static final int ARRAY_INDEX1 = 9;
    private static final int ARRAY_INDEX2 = 10;
    private static final int MAP_INDEX1 = 11;
    private static final int MAP_INDEX2 = 12;

    private MetaStore metaStore;
    private CodeGenRunner codeGenRunner;
    private InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    private GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;


    @Before
    public void init() {
        metaStore = MetaStoreFixture.getNewMetaStore(functionRegistry);
        // load substring function
        UdfLoaderUtil.load(metaStore);

        final Schema schema = SchemaBuilder.struct()
            .field("CODEGEN_TEST.COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field("CODEGEN_TEST.COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("CODEGEN_TEST.COL2", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("CODEGEN_TEST.COL3", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
            .field("CODEGEN_TEST.COL4", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
            .field("CODEGEN_TEST.COL5", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
            .field("CODEGEN_TEST.COL6", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
            .field("CODEGEN_TEST.COL7", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
            .field("CODEGEN_TEST.COL8", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field("CODEGEN_TEST.COL9", SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA).optional().build())
            .field("CODEGEN_TEST.COL10", SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA).optional().build())
            .field("CODEGEN_TEST.COL11",
                   SchemaBuilder.map(SchemaBuilder.OPTIONAL_STRING_SCHEMA, SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build())
            .field("CODEGEN_TEST.COL12",
                   SchemaBuilder.map(SchemaBuilder.OPTIONAL_STRING_SCHEMA, SchemaBuilder.OPTIONAL_INT32_SCHEMA).optional().build())
            .field("CODEGEN_TEST.COL13", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build());
        final Schema metaStoreSchema = SchemaBuilder.struct()
            .field("COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field("COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("COL2", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("COL3", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
            .field("COL4", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
            .field("COL5", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
            .field("COL6", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
            .field("COL7", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
            .field("COL8", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field("COL9", SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA).optional().build())
            .field("COL10", SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA).optional().build())
            .field("COL11",
                SchemaBuilder.map(SchemaBuilder.OPTIONAL_STRING_SCHEMA, SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build())
            .field("COL12",
                SchemaBuilder.map(SchemaBuilder.OPTIONAL_STRING_SCHEMA, SchemaBuilder.OPTIONAL_INT32_SCHEMA).optional().build())
            .field("COL13", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build());
        final KsqlTopic ksqlTopic = new KsqlTopic(
            "CODEGEN_TEST",
            "codegen_test",
            new KsqlJsonTopicSerDe());
        final KsqlStream ksqlStream = new KsqlStream(
            "sqlexpression",
            "CODEGEN_TEST", metaStoreSchema,
            metaStoreSchema.field("COL0"),
            null,
            ksqlTopic);
        metaStore.putTopic(ksqlTopic);
        metaStore.putSource(ksqlStream);
        codeGenRunner = new CodeGenRunner(schema, functionRegistry);
        genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
    }

    @Test
    public void testNullEquals() throws Exception {
        assertThat(evalBooleanExprEq(INT32_INDEX1, INT64_INDEX1, new Object[]{null, 12344L}),is(false));
        assertThat(evalBooleanExprEq(INT32_INDEX1, INT64_INDEX1, new Object[]{null, null}), is(false));
    }

    @Test
    public void testIsDistinctFrom() throws Exception {
        assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{12344, 12344L}), is(false));
        assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12344L}), is(true));
        assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{null, 12344L}), is(true));
        assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{null, null}), is(false));
    }

    @Test
    public void testIsNull() throws Exception {
        final String simpleQuery = "SELECT col0 IS NULL FROM CODEGEN_TEST;";
        final Analysis analysis = analyzeQuery(simpleQuery);

        final ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0));
        assertThat(expressionEvaluatorMetadata0.getIndexes().length, equalTo(1));
        final int idx0 = expressionEvaluatorMetadata0.getIndexes()[0];
        assertThat(idx0, equalTo(0));
        assertThat(expressionEvaluatorMetadata0.getUdfs().length, equalTo(1));

        Object result0 = expressionEvaluatorMetadata0.evaluate(new Object[]{null});
        assertThat(result0, instanceOf(Boolean.class));
        assertThat(result0, is(true));

        result0 = expressionEvaluatorMetadata0.evaluate(new Object[]{12345L});
        assertThat(result0, instanceOf(Boolean.class));
        assertThat(result0, is(false));
    }

    @Test
    public void testIsNotNull() throws Exception {
        final String simpleQuery = "SELECT col0 IS NOT NULL FROM CODEGEN_TEST;";
        final Analysis analysis = analyzeQuery(simpleQuery);

        final ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0));
        assertThat(expressionEvaluatorMetadata0.getIndexes().length, equalTo(1));
        final int idx0 = expressionEvaluatorMetadata0.getIndexes()[0];
        assertThat(idx0, equalTo(0));
        assertThat(expressionEvaluatorMetadata0.getUdfs().length, equalTo(1));

        Object result0 = expressionEvaluatorMetadata0.evaluate(new Object[]{null});
        assertThat(result0, instanceOf(Boolean.class));
        assertThat(result0, is(false));

        result0 = expressionEvaluatorMetadata0.evaluate(new Object[]{12345L});
        assertThat(result0, instanceOf(Boolean.class));
        assertThat(result0, is(true));
    }

    @Test
    public void testBooleanExprScalarEq() throws Exception {
        // int32
        assertThat(evalBooleanExprEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12344L}), is(false));
        assertThat(evalBooleanExprEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12345L}), is(true));
        // int64
        assertThat(evalBooleanExprEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12344}), is(false));
        assertThat(evalBooleanExprEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12345}), is(true));
        // double
        assertThat(evalBooleanExprEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12345.0, 12344.0}), is(false));
        assertThat(evalBooleanExprEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12345.0, 12345.0}), is(true));
    }

    @Test
    public void testBooleanExprBooleanEq() throws Exception {
        assertThat(evalBooleanExprEq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{false, true}), is(false));
        assertThat(evalBooleanExprEq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{true, true}), is(true));
    }

    @Test
    public void testBooleanExprStringEq() throws Exception {
        assertThat(evalBooleanExprEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(false));
        assertThat(evalBooleanExprEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(true));
    }

    @Test
    public void testBooleanExprArrayComparisonFails() throws Exception {
        final Integer[] a1 = new Integer[]{1, 2, 3};
        final Integer[] a2 = new Integer[]{1, 2, 3};
        try {
            evalBooleanExprEq(ARRAY_INDEX1, ARRAY_INDEX2, new Object[]{a1, a2});
            Assert.fail("Array comparison should throw exception");
        } catch (final KsqlException e) {
            assertThat(e.getMessage(), equalTo("Cannot compare ARRAY values"));
        }
    }

    @Test
    public void testBooleanExprMapComparisonFails() throws Exception {
        final HashMap<Integer, Integer> a1 = new HashMap<>();
        a1.put(1, 2);
        final HashMap<Integer, Integer> a2 = new HashMap<>(a1);

        try {
            evalBooleanExprEq(MAP_INDEX1, MAP_INDEX2, new Object[]{a1, a2});
        } catch (final KsqlException e) {
            assertThat(e.getMessage(), equalTo("Cannot compare MAP values"));
        }
    }

    @Test
    public void testBooleanExprScalarNeq() throws Exception {
        // int32
        assertThat(evalBooleanExprNeq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12344L}), is(true));
        assertThat(evalBooleanExprNeq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12345L}), is(false));
        // int64
        assertThat(evalBooleanExprNeq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12344}), is(true));
        assertThat(evalBooleanExprNeq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12345}), is(false));
        // double
        assertThat(evalBooleanExprNeq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12345.0, 12344.0}), is(true));
        assertThat(evalBooleanExprNeq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12345.0, 12345.0}), is(false));
    }

    @Test
    public void testBooleanExprBooleanNeq() throws Exception {
        assertThat(evalBooleanExprNeq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{false, true}), is(true));
        assertThat(evalBooleanExprNeq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{true, true}), is(false));
    }

    @Test
    public void testBooleanExprStringNeq() throws Exception {
        assertThat(evalBooleanExprNeq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(true));
        assertThat(evalBooleanExprNeq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(false));
    }

    @Test
    public void testBooleanExprScalarLessThan() throws Exception {
        // int32
        assertThat(evalBooleanExprLessThan(INT32_INDEX1, INT64_INDEX1, new Object[]{12344, 12345L}), is(true));
        assertThat(evalBooleanExprLessThan(INT32_INDEX1, INT64_INDEX1, new Object[]{12346, 12345L}), is(false));
        // int64
        assertThat(evalBooleanExprLessThan(INT64_INDEX2, INT32_INDEX1, new Object[]{12344L, 12345}), is(true));
        assertThat(evalBooleanExprLessThan(INT64_INDEX2, INT32_INDEX1, new Object[]{12346L, 12345}), is(false));
        // double
        assertThat(evalBooleanExprLessThan(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12344.0, 12345.0}), is(true));
        assertThat(evalBooleanExprLessThan(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12346.0, 12345.0}), is(false));
    }

    @Test
    public void testBooleanExprStringLessThan() throws Exception {
        assertThat(evalBooleanExprLessThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(true));
        assertThat(evalBooleanExprLessThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(false));
    }

    @Test
    public void testBooleanExprScalarLessThanEq() throws Exception {
        // int32
        assertThat(evalBooleanExprLessThanEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12345L}), is(true));
        assertThat(evalBooleanExprLessThanEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12346, 12345L}), is(false));
        // int64
        assertThat(evalBooleanExprLessThanEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12345}), is(true));
        assertThat(evalBooleanExprLessThanEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12346L, 12345}), is(false));
        // double
        assertThat(evalBooleanExprLessThanEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12344.0, 12345.0}), is(true));
        assertThat(evalBooleanExprLessThanEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12346.0, 12345.0}), is(false));
    }

    @Test
    public void testBooleanExprStringLessThanEq() throws Exception {
        assertThat(evalBooleanExprLessThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(true));
        assertThat(evalBooleanExprLessThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abb"}), is(false));
    }

    @Test
    public void testBooleanExprScalarGreaterThan() throws Exception {
        // int32
        assertThat(evalBooleanExprGreaterThan(INT32_INDEX1, INT64_INDEX1, new Object[]{12346, 12345L}), is(true));
        assertThat(evalBooleanExprGreaterThan(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12345L}), is(false));
        // int64
        assertThat(evalBooleanExprGreaterThan(INT64_INDEX2, INT32_INDEX1, new Object[]{12346L, 12345}), is(true));
        assertThat(evalBooleanExprGreaterThan(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12345}), is(false));
        // double
        assertThat(evalBooleanExprGreaterThan(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12346.0, 12345.0}), is(true));
        assertThat(evalBooleanExprGreaterThan(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12344.0, 12345.0}), is(false));
    }

    @Test
    public void testBooleanExprStringGreaterThan() throws Exception {
        assertThat(evalBooleanExprGreaterThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"def", "abc"}), is(true));
        assertThat(evalBooleanExprGreaterThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(false));
    }

    @Test
    public void testBooleanExprScalarGreaterThanEq() throws Exception {
        // int32
        assertThat(evalBooleanExprGreaterThanEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12345L}), is(true));
        assertThat(evalBooleanExprGreaterThanEq(INT32_INDEX1, INT64_INDEX1, new Object[]{12344, 12345L}), is(false));
        // int64
        assertThat(evalBooleanExprGreaterThanEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12345L, 12345}), is(true));
        assertThat(evalBooleanExprGreaterThanEq(INT64_INDEX2, INT32_INDEX1, new Object[]{12344L, 12345}), is(false));
        // double
        assertThat(evalBooleanExprGreaterThanEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12346.0, 12345.0}), is(true));
        assertThat(evalBooleanExprGreaterThanEq(FLOAT64_INDEX2, FLOAT64_INDEX1, new Object[]{12344.0, 12345.0}), is(false));
    }

    @Test
    public void testBooleanExprStringGreaterThanEq() throws Exception {
        assertThat(evalBooleanExprGreaterThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"def", "abc"}), is(true));
        assertThat(evalBooleanExprGreaterThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(false));
    }

    @Test
    public void shouldHandleArithmeticExpr() {
        // Given:
        final String query =
            "SELECT col0+col3, col3+10, col0*25, 12*4+2 FROM codegen_test WHERE col0 > 100;";

        final Map<Integer, Object> inputValues = ImmutableMap.of(0, 5L, 3, 15.0);

        // When:
        final List<Object> columns = executeExpression(query, inputValues);

        // Then:
        assertThat(columns, contains(20.0, 25.0, 125L, 50));
    }

    @Test
    public void shouldHandleMathUdfs() {
        // Given:
        final String query =
            "SELECT FLOOR(col3), CEIL(col3*3), ABS(col0+1.34), ROUND(col3*2)+12 FROM codegen_test;";

        final Map<Integer, Object> inputValues = ImmutableMap.of(0, 15, 3, 1.5);

        // When:
        final List<Object> columns = executeExpression(query, inputValues);

        // Then:
        assertThat(columns, contains(1.0, 5.0, 16.34, 15L));
    }

    @Test
    public void shouldHandleRandomUdf() {
        // Given:
        final String query = "SELECT RANDOM()+10, RANDOM()+col0 FROM codegen_test;";
        final Map<Integer, Object> inputValues = ImmutableMap.of(0, 15);

        // When:
        final List<Object> columns = executeExpression(query, inputValues);

        // Then:
        assertThat(columns.get(0), is(instanceOf(Double.class)));
        assertThat((Double)columns.get(0),
                   is(both(greaterThanOrEqualTo(10.0)).and(lessThanOrEqualTo(11.0))));

        assertThat(columns.get(1), is(instanceOf(Double.class)));
        assertThat((Double)columns.get(1),
                   is(both(greaterThanOrEqualTo(15.0)).and(lessThanOrEqualTo(16.0))));
    }

    @Test
    public void shouldHandleStringUdfs() {
        // Given:
        final String query =
            "SELECT LCASE(col1), UCASE(col1), TRIM(col1), CONCAT(col1,'_test'), SUBSTRING(col1, 1, 3)"
            + " FROM codegen_test;";

        final Map<Integer, Object> inputValues = ImmutableMap.of(1, " Hello ");

        // When:
        final List<Object> columns = executeExpression(query, inputValues);

        // Then:
        assertThat(columns, contains(" hello ", " HELLO ", "Hello", " Hello _test", "He"));
    }

    @Test
    public void shouldHandleNestedUdfs() {
        final String query =
            "SELECT "
            + "CONCAT(EXTRACTJSONFIELD(col1,'$.name'),CONCAT('-',EXTRACTJSONFIELD(col1,'$.value')))"
            + " FROM codegen_test;";

        final Map<Integer, Object> inputValues = ImmutableMap.of(1, "{\"name\":\"fred\",\"value\":1}");

        // When:
        executeExpression(query, inputValues);
    }

    @Test
    public void shouldHandleMaps() throws Exception {
        final String query =
            "SELECT col11['address'] as Address FROM codegen_test;";

        final Map<String, String> inputs = new HashMap<>();
        inputs.put("address", "{\"city\":\"adelaide\",\"country\":\"oz\"}");

        final Analysis analysis = analyzeQuery(query);
        final ExpressionMetadata expressionMetadata
            = codeGenRunner.buildCodeGenFromParseTree(analysis.getSelectExpressions().get(0));

        assertThat(expressionMetadata.evaluate(new Object[]{inputs}),
            equalTo("{\"city\":\"adelaide\",\"country\":\"oz\"}"));
    }

    @Test
    public void shouldHandleUdfsExtractingFromMaps() throws Exception {
        final String query =
            "SELECT EXTRACTJSONFIELD(col11['address'], '$.city') FROM codegen_test;";

        final Map<String, String> inputs = new HashMap<>();
        inputs.put("address", "{\"city\":\"adelaide\",\"country\":\"oz\"}");

        final Analysis analysis = analyzeQuery(query);
        final ExpressionMetadata metadata
            = codeGenRunner.buildCodeGenFromParseTree(analysis.getSelectExpressions().get(0));

        final Object [] params = new Object[2];
        for (int i = 0; i < 2; i++) {
            if (metadata.getIndexes()[i] == -1) {
                params[i] = metadata.getUdfs()[i];
            } else {
                params[i] = inputs;
            }
        }
        assertThat(metadata.evaluate(params), equalTo("adelaide"));
    }

    @Test
    public void shouldHandleFunctionWithNullArgument() {
        final String query =
            "SELECT test_udf(col0, NULL) FROM codegen_test;";

        final Map<Integer, Object> inputValues = ImmutableMap.of(0, 0);
        final List<Object> columns = executeExpression(query, inputValues);
        // test
        assertThat(columns, equalTo(Collections.singletonList("doStuffLongString")));
    }

    @Test
    public void shouldChoseFunctionWithCorrectNumberOfArgsWhenNullArgument() {
        final String query =
            "SELECT test_udf(col0, col0, NULL) FROM codegen_test;";

        final Map<Integer, Object> inputValues = ImmutableMap.of(0, 0);
        final List<Object> columns = executeExpression(query, inputValues);
        // test
        assertThat(columns, equalTo(Collections.singletonList("doStuffLongLongString")));
    }

    private List<Object> executeExpression(final String query,
                                           final Map<Integer, Object> inputValues) {
        final Analysis analysis = analyzeQuery(query);

        final Function<Expression, ExpressionMetadata> buildCodeGenFromParseTree =
            exp -> {
                try {
                    return codeGenRunner.buildCodeGenFromParseTree(exp);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            };

        return analysis.getSelectExpressions().stream()
            .map(buildCodeGenFromParseTree)
            .map(md -> md.evaluate(buildParams(md, inputValues)))
            .collect(Collectors.toList());
    }

    private Analysis analyzeQuery(final String queryStr) {
        final List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
        final Analysis analysis = new Analysis();
        final Analyzer analyzer = new Analyzer(queryStr, analysis, metaStore, "");
        analyzer.process(statements.get(0), new AnalysisContext(null));
        return analysis;
    }

    private boolean evalBooleanExprEq(final int cola, final int colb, final Object[] values) throws Exception {
        return evalBooleanExpr("SELECT col%d = col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprNeq(final int cola, final int colb, final Object[] values) throws Exception {
        return evalBooleanExpr("SELECT col%d != col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprIsDistinctFrom(final int cola, final int colb, final Object[] values) throws Exception {
        return evalBooleanExpr("SELECT col%d IS DISTINCT FROM col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprLessThan(final int cola, final int colb, final Object[] values) throws Exception {
        return evalBooleanExpr("SELECT col%d < col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprLessThanEq(final int cola, final int colb, final Object[] values) throws Exception {
        return evalBooleanExpr("SELECT col%d <= col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprGreaterThan(final int cola, final int colb, final Object[] values) throws Exception {
        return evalBooleanExpr("SELECT col%d > col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprGreaterThanEq(final int cola, final int colb, final Object[] values) throws Exception {
        return evalBooleanExpr("SELECT col%d >= col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExpr(
        final String queryFormat, final int cola, final int colb, final Object[] values)
        throws Exception {
        final String simpleQuery = String.format(queryFormat, cola, colb);
        final Analysis analysis = analyzeQuery(simpleQuery);

        final ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0));
        assertThat(expressionEvaluatorMetadata0.getIndexes().length, equalTo(2));
        final int idx0 = expressionEvaluatorMetadata0.getIndexes()[0];
        final int idx1 = expressionEvaluatorMetadata0.getIndexes()[1];
        assertThat(idx0, anyOf(equalTo(cola), equalTo(colb)));
        assertThat(idx1, anyOf(equalTo(cola), equalTo(colb)));
        assertThat(idx0, not(equalTo(idx1)));
        if (idx0 == colb) {
            final Object tmp = values[0];
            values[0] = values[1];
            values[1] = tmp;
        }
        assertThat(expressionEvaluatorMetadata0.getUdfs().length, equalTo(2));
        final Object result0 = expressionEvaluatorMetadata0.evaluate(values);
        assertThat(result0, instanceOf(Boolean.class));
        return (Boolean)result0;
    }

    private Object[] buildParams(final ExpressionMetadata metadata,
                                 final Map<Integer, Object> inputValues) {
        final Kudf[] udfs = metadata.getUdfs();
        final Object[] params = new Object[udfs.length];

        int argsIdx = 0;
        for (final int i : metadata.getIndexes()) {
            if (i == -1) {
                params[argsIdx] = udfs[argsIdx++];
            } else {
                final Object param = genericRowValueTypeEnforcer.enforceFieldType(i, inputValues.get(i));
                params[argsIdx++] = param;
            }
        }

        return params;
    }
}
