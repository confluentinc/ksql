/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.codegen;

import static io.confluent.ksql.testutils.AnalysisTestUtil.analyzeQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UdfLoaderUtil;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


@SuppressWarnings("SameParameterValue")
public class CodeGenRunnerTest {

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
    private static final int STRUCT_INDEX = 15;

    private static final Schema STRUCT_SCHEMA =
        SchemaBuilder.struct().optional().field("A", Schema.OPTIONAL_STRING_SCHEMA).build();

    private static final List<Object> ONE_ROW = ImmutableList.of(
        0L, "S1", "S2", 3.1, 4.2, 5, true, false, 8L,
        ImmutableList.of(1, 2), ImmutableList.of(2, 4),
        ImmutableMap.of("key1", "value1", "address", "{\"city\":\"adelaide\",\"country\":\"oz\"}"),
        ImmutableMap.of("k1", 4),
        ImmutableList.of("one", "two"),
        ImmutableList.of(ImmutableList.of("1", "2"), ImmutableList.of("3")),
        new Struct(STRUCT_SCHEMA).put("A", "VALUE"));

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private MutableMetaStore metaStore;
    private CodeGenRunner codeGenRunner;
    private final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());

    @Before
    public void init() {
        final KsqlFunction whenCondition = KsqlFunction.createLegacyBuiltIn(
            Schema.OPTIONAL_BOOLEAN_SCHEMA,
            ImmutableList.of(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA),
            "WHENCONDITION",
            WhenCondition.class
        );
        final KsqlFunction whenResult = KsqlFunction.createLegacyBuiltIn(
            Schema.OPTIONAL_INT32_SCHEMA,
            ImmutableList.of(Schema.OPTIONAL_INT32_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA),
            "WHENRESULT",
            WhenResult.class
        );
        functionRegistry.ensureFunctionFactory(
            UdfLoaderUtil.createTestUdfFactory(whenCondition));
        functionRegistry.addFunction(whenCondition);
        functionRegistry.ensureFunctionFactory(
            UdfLoaderUtil.createTestUdfFactory(whenResult));
        functionRegistry.addFunction(whenResult);
        metaStore = MetaStoreFixture.getNewMetaStore(functionRegistry);
        // load substring function
        UdfLoaderUtil.load(functionRegistry);

        final Schema arraySchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();


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
            .field("CODEGEN_TEST.COL13", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build())
            .field("CODEGEN_TEST.COL14", SchemaBuilder.array(arraySchema).optional().build())
            .field("CODEGEN_TEST.COL15", STRUCT_SCHEMA)
            .build();

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
            .field("COL13", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build())
            .field("COL14", SchemaBuilder.array(arraySchema).optional().build())
            .field("COL15", STRUCT_SCHEMA)
            .build();

        final KsqlTopic ksqlTopic = new KsqlTopic(
            "CODEGEN_TEST",
            "codegen_test",
            new KsqlJsonSerdeFactory(), false);

        final KsqlStream ksqlStream = new KsqlStream<>(
            "sqlexpression",
            "CODEGEN_TEST",
            LogicalSchema.of(metaStoreSchema),
            SerdeOption.none(),
            KeyField.of("COL0", metaStoreSchema.field("COL0")),
            new MetadataTimestampExtractionPolicy(),
            ksqlTopic,
            Serdes::String
        );

        metaStore.putSource(ksqlStream);
        codeGenRunner = new CodeGenRunner(LogicalSchema.of(schema), ksqlConfig, functionRegistry);
    }

    @Test
    public void testNullEquals() {
        assertThat(evalBooleanExprEq(INT32_INDEX1, INT64_INDEX1, new Object[]{null, 12344L}),is(false));
        assertThat(evalBooleanExprEq(INT32_INDEX1, INT64_INDEX1, new Object[]{null, null}), is(false));
    }

    @Test
    public void testIsDistinctFrom() {
        assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{12344, 12344L}), is(false));
        assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{12345, 12344L}), is(true));
        assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{null, 12344L}), is(true));
        assertThat(evalBooleanExprIsDistinctFrom(INT32_INDEX1, INT64_INDEX1, new Object[]{null, null}), is(false));
    }

    @Test
    public void testIsNull() {
        final String simpleQuery = "SELECT col0 IS NULL FROM CODEGEN_TEST;";
        final Analysis analysis = analyzeQuery(simpleQuery, metaStore);

        final ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0), "Select");
        assertThat(expressionEvaluatorMetadata0.getIndexes(), contains(0));
        assertThat(expressionEvaluatorMetadata0.getUdfs(), hasSize(1));

        Object result0 = expressionEvaluatorMetadata0.evaluate(genericRow(null, 1));
        assertThat(result0, is(true));

        result0 = expressionEvaluatorMetadata0.evaluate(genericRow(12345L));
        assertThat(result0, is(false));
    }

    @Test
    public void shouldHandleMultiDimensionalArray() {
        // Given:
        final String simpleQuery = "SELECT col14[0][0] FROM CODEGEN_TEST;";
        final Analysis analysis = analyzeQuery(simpleQuery, metaStore);

        // When:
        final Object result = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0), "Select")
            .evaluate(genericRow(ONE_ROW));

        // Then:
        assertThat(result, is("1"));
    }

    @Test
    public void testIsNotNull() {
        final String simpleQuery = "SELECT col0 IS NOT NULL FROM CODEGEN_TEST;";
        final Analysis analysis = analyzeQuery(simpleQuery, metaStore);

        final ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0), "Filter");
        assertThat(expressionEvaluatorMetadata0.getIndexes(), contains(0));
        assertThat(expressionEvaluatorMetadata0.getUdfs(), hasSize(1));

        Object result0 = expressionEvaluatorMetadata0.evaluate(genericRow(null, "1"));
        assertThat(result0, is(false));

        result0 = expressionEvaluatorMetadata0.evaluate(genericRow(12345L));
        assertThat(result0, is(true));
    }

    @Test
    public void testBooleanExprScalarEq() {
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
    public void testBooleanExprBooleanEq() {
        assertThat(evalBooleanExprEq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{false, true}), is(false));
        assertThat(evalBooleanExprEq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{true, true}), is(true));
    }

    @Test
    public void testBooleanExprStringEq() {
        assertThat(evalBooleanExprEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(false));
        assertThat(evalBooleanExprEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(true));
    }

    @Test
    public void testBooleanExprArrayComparisonFails() {
        // Given:
        expectedException.expect(KsqlException.class);
        expectedException.expectMessage("Code generation failed for Filter: "
            + "Cannot compare ARRAY values. "
            + "expression:(CODEGEN_TEST.COL9 = CODEGEN_TEST.COL10)");
        expectedException.expectCause(hasMessage(equalTo("Cannot compare ARRAY values")));

        // When:
        evalBooleanExprEq(ARRAY_INDEX1, ARRAY_INDEX2,
            new Object[]{new Integer[]{1}, new Integer[]{1}});
    }

    @Test
    public void testBooleanExprMapComparisonFails() {
        // Given:
        expectedException.expect(KsqlException.class);
        expectedException.expectMessage("Code generation failed for Filter: "
            + "Cannot compare MAP values. "
            + "expression:(CODEGEN_TEST.COL11 = CODEGEN_TEST.COL12)");
        expectedException.expectCause(hasMessage(equalTo("Cannot compare MAP values")));

        // When:
        evalBooleanExprEq(MAP_INDEX1, MAP_INDEX2,
            new Object[]{ImmutableMap.of(1, 2), ImmutableMap.of(1, 2)});
    }

    @Test
    public void testBooleanExprScalarNeq() {
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
    public void testBooleanExprBooleanNeq() {
        assertThat(evalBooleanExprNeq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{false, true}), is(true));
        assertThat(evalBooleanExprNeq(BOOLEAN_INDEX2, BOOLEAN_INDEX1, new Object[]{true, true}), is(false));
    }

    @Test
    public void testBooleanExprStringNeq() {
        assertThat(evalBooleanExprNeq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(true));
        assertThat(evalBooleanExprNeq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(false));
    }

    @Test
    public void testBooleanExprScalarLessThan() {
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
    public void testBooleanExprStringLessThan() {
        assertThat(evalBooleanExprLessThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(true));
        assertThat(evalBooleanExprLessThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(false));
    }

    @Test
    public void testBooleanExprScalarLessThanEq() {
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
    public void testBooleanExprStringLessThanEq() {
        assertThat(evalBooleanExprLessThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(true));
        assertThat(evalBooleanExprLessThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abb"}), is(false));
    }

    @Test
    public void testBooleanExprScalarGreaterThan() {
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
    public void testBooleanExprStringGreaterThan() {
        assertThat(evalBooleanExprGreaterThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"def", "abc"}), is(true));
        assertThat(evalBooleanExprGreaterThan(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "abc"}), is(false));
    }

    @Test
    public void testBooleanExprScalarGreaterThanEq() {
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
    public void testBooleanExprStringGreaterThanEq() {
        assertThat(evalBooleanExprGreaterThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"def", "abc"}), is(true));
        assertThat(evalBooleanExprGreaterThanEq(STRING_INDEX1, STRING_INDEX2, new Object[]{"abc", "def"}), is(false));
    }

    @Test
    public void testBetweenExprScalar() {
        // int
        assertThat(evalBetweenClauseScalar(INT32_INDEX1, 1, 0, 2), is(true));
        assertThat(evalBetweenClauseScalar(INT32_INDEX1, 0, 0, 2), is(true));
        assertThat(evalBetweenClauseScalar(INT32_INDEX1, 3, 0, 2), is(false));
        assertThat(evalBetweenClauseScalar(INT32_INDEX1, null, 0, 2), is(false));

        // long
        assertThat(evalBetweenClauseScalar(INT64_INDEX1, 12345L, 12344L, 12346L), is(true));
        assertThat(evalBetweenClauseScalar(INT64_INDEX1, 12344L, 12344L, 12346L), is(true));
        assertThat(evalBetweenClauseScalar(INT64_INDEX1, 12345L, 0, 2L), is(false));
        assertThat(evalBetweenClauseScalar(INT64_INDEX1, null, 0, 2L), is(false));

        // double
        assertThat(evalBetweenClauseScalar(FLOAT64_INDEX1, 1.0d, 0.1d, 1.9d), is(true));
        assertThat(evalBetweenClauseScalar(FLOAT64_INDEX1, 0.1d, 0.1d, 1.9d), is(true));
        assertThat(evalBetweenClauseScalar(FLOAT64_INDEX1, 2.0d, 0.1d, 1.9d), is(false));
        assertThat(evalBetweenClauseScalar(FLOAT64_INDEX1, null, 0.1d, 1.9d), is(false));
    }

    @Test
    public void testNotBetweenScalar() {
        // int
        assertThat(evalNotBetweenClauseScalar(INT32_INDEX1, 1, 0, 2), is(false));
        assertThat(evalNotBetweenClauseScalar(INT32_INDEX1, 0, 0, 2), is(false));
        assertThat(evalNotBetweenClauseScalar(INT32_INDEX1, 3, 0, 2), is(true));
        assertThat(evalNotBetweenClauseScalar(INT32_INDEX1, null, 0, 2), is(true));

        // long
        assertThat(evalNotBetweenClauseScalar(INT64_INDEX1, 12345L, 12344L, 12346L), is(false));
        assertThat(evalNotBetweenClauseScalar(INT64_INDEX1, 12344L, 12344L, 12346L), is(false));
        assertThat(evalNotBetweenClauseScalar(INT64_INDEX1, 12345L, 0, 2L), is(true));
        assertThat(evalNotBetweenClauseScalar(INT64_INDEX1, null, 0, 2L), is(true));

        // double
        assertThat(evalNotBetweenClauseScalar(FLOAT64_INDEX1, 1.0d, 0.1d, 1.9d), is(false));
        assertThat(evalNotBetweenClauseScalar(FLOAT64_INDEX1, 0.1d, 0.1d, 1.9d), is(false));
        assertThat(evalNotBetweenClauseScalar(FLOAT64_INDEX1, 2.0d, 0.1d, 1.9d), is(true));
        assertThat(evalNotBetweenClauseScalar(FLOAT64_INDEX1, null, 0.1d, 1.9d), is(true));
    }

    @Test
    public void testBetweenExprString() {
        // constants
        assertThat(evalBetweenClauseString(STRING_INDEX1, "b", "'a'", "'c'"), is(true));
        assertThat(evalBetweenClauseString(STRING_INDEX1, "a", "'a'", "'c'"), is(true));
        assertThat(evalBetweenClauseString(STRING_INDEX1, "d", "'a'", "'c'"), is(false));
        assertThat(evalBetweenClauseString(STRING_INDEX1, null, "'a'", "'c'"), is(false));

        // columns
        assertThat(evalBetweenClauseString(STRING_INDEX1, "S2", "col" + STRING_INDEX2, "'S3'"), is(true));
        assertThat(evalBetweenClauseString(STRING_INDEX1, "S3", "col" + STRING_INDEX2, "'S3'"), is(true));
        assertThat(evalBetweenClauseString(STRING_INDEX1, "S4", "col" + STRING_INDEX2, "'S3'"), is(false));
        assertThat(evalBetweenClauseString(STRING_INDEX1, null, "col" + STRING_INDEX2, "'S3'"), is(false));
    }

    @Test
    public void testNotBetweenExprString() {
        // constants
        assertThat(evalNotBetweenClauseString(STRING_INDEX1, "b", "'a'", "'c'"), is(false));
        assertThat(evalNotBetweenClauseString(STRING_INDEX1, "a", "'a'", "'c'"), is(false));
        assertThat(evalNotBetweenClauseString(STRING_INDEX1, "d", "'a'", "'c'"), is(true));
        assertThat(evalNotBetweenClauseString(STRING_INDEX1, null, "'a'", "'c'"), is(true));

        // columns
        assertThat(evalNotBetweenClauseString(STRING_INDEX1, "S2", "col" + STRING_INDEX2, "'S3'"), is(false));
        assertThat(evalNotBetweenClauseString(STRING_INDEX1, "S3", "col" + STRING_INDEX2, "'S3'"), is(false));
        assertThat(evalNotBetweenClauseString(STRING_INDEX1, "S4", "col" + STRING_INDEX2, "'S3'"), is(true));
        assertThat(evalNotBetweenClauseString(STRING_INDEX1, null, "col" + STRING_INDEX2, "'S3'"), is(true));
    }

    @Test
    public void testInvalidBetweenArrayValue() {
        // Given:
        expectedException.expect(KsqlException.class);
        expectedException.expectMessage("Code generation failed for Filter: "
            + "Cannot execute BETWEEN with ARRAY values. "
            + "expression:(NOT (CODEGEN_TEST.COL9 BETWEEN 'a' AND 'c'))");
        expectedException.expectCause(hasMessage(
            equalTo("Cannot execute BETWEEN with ARRAY values")));

        // When:
        evalNotBetweenClauseObject(ARRAY_INDEX1, new Object[]{1, 2}, "'a'", "'c'");
    }

    @Test
    public void testInvalidBetweenMapValue() {
        // Given:
        expectedException.expect(KsqlException.class);
        expectedException.expectMessage("Code generation failed for Filter: "
            + "Cannot execute BETWEEN with MAP values. "
            + "expression:(NOT (CODEGEN_TEST.COL11 BETWEEN 'a' AND 'c'))");
        expectedException.expectCause(hasMessage(
            equalTo("Cannot execute BETWEEN with MAP values")));

        // When:
        evalNotBetweenClauseObject(MAP_INDEX1, ImmutableMap.of(1, 2), "'a'", "'c'");
    }

    @Test
    public void testInvalidBetweenBooleanValue() {
        // Given:
        expectedException.expect(KsqlException.class);
        expectedException.expectMessage("Code generation failed for Filter: "
            + "Cannot execute BETWEEN with BOOLEAN values. "
            + "expression:(NOT (CODEGEN_TEST.COL6 BETWEEN 'a' AND 'c'))");
        expectedException.expectCause(hasMessage(
            equalTo("Cannot execute BETWEEN with BOOLEAN values")));

        // When:
        evalNotBetweenClauseObject(BOOLEAN_INDEX1, true, "'a'", "'c'");
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
    public void testCastNumericArithmeticExpressions() {
        final Map<Integer, Object> inputValues =
            ImmutableMap.of(0, 1, 3, 3, 4, 4, 5, 5);

        // INT64 - INT32
        assertThat(executeExpression(
            "SELECT "
                + "CAST((col5 - col0) AS INTEGER),"
                + "CAST((col5 - col0) AS BIGINT),"
                + "CAST((col5 - col0) AS DOUBLE),"
                + "CAST((col5 - col0) AS STRING)"
                + "FROM codegen_test;",
            inputValues), contains(4, 4L, 4.0, "4"));

        // FLOAT64 - FLOAT64
        assertThat(executeExpression(
            "SELECT "
                + "CAST((col4 - col3) AS INTEGER),"
                + "CAST((col4 - col3) AS BIGINT),"
                + "CAST((col4 - col3) AS DOUBLE),"
                + "CAST((col4 - col3) AS STRING)"
                + "FROM codegen_test;",
            inputValues), contains(1, 1L, 1.0, "1.0"));

        // FLOAT64 - INT64
        assertThat(executeExpression(
            "SELECT "
                + "CAST((col4 - col0) AS INTEGER),"
                + "CAST((col4 - col0) AS BIGINT),"
                + "CAST((col4 - col0) AS DOUBLE),"
                + "CAST((col4 - col0) AS STRING)"
                + "FROM codegen_test;",
            inputValues), contains(3, 3L, 3.0, "3.0"));
    }

    @Test
    public void shouldHandleStringLiteralWithCharactersThatMustBeEscaped() {
        // Given:
        final String query = "SELECT CONCAT(CONCAT('\\\"', 'foo'), '\\\"') FROM CODEGEN_TEST;";

        // When:
        final List<Object> columns = executeExpression(query, Collections.emptyMap());

        // Then:
        assertThat(columns, contains("\\\"foo\\\""));
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
            "SELECT LCASE(col1), UCASE(col1), TRIM(col1), CONCAT(col1,'_test'), SUBSTRING(col1, 2, 4)"
            + " FROM codegen_test;";

        final Map<Integer, Object> inputValues = ImmutableMap.of(1, " Hello ");

        // When:
        final List<Object> columns = executeExpression(query, inputValues);

        // Then:
        assertThat(columns, contains(" hello ", " HELLO ", "Hello", " Hello _test", "Hell"));
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
    public void shouldHandleMaps() {
        // Given:
        final Expression expression = analyzeQuery(
            "SELECT col11['key1'] as Address FROM codegen_test;", metaStore)
            .getSelectExpressions()
            .get(0);

        // When:
        final Object result = codeGenRunner
            .buildCodeGenFromParseTree(expression, "Group By")
            .evaluate(genericRow(ONE_ROW));

        // Then:
        assertThat(result, is("value1"));
    }

    @Test
    public void shouldHandleCaseStatement() {
        // Given:
        final Expression expression = analyzeQuery(
            "SELECT CASE "
                + "     WHEN col0 < 10 THEN 'small' "
                + "     WHEN col0 < 100 THEN 'medium' "
                + "     ELSE 'large' "
                + "END "
                + "FROM codegen_test;", metaStore)
            .getSelectExpressions()
            .get(0);

        // When:
        final Object result = codeGenRunner
            .buildCodeGenFromParseTree(expression, "Case")
            .evaluate(genericRow(ONE_ROW));

        // Then:
        assertThat(result, is("small"));
    }

    @Test
    public void shouldHandleCaseStatementLazily() {
        // Given:
        final Expression expression = analyzeQuery(
            "SELECT CASE "
                + "     WHEN WHENCONDITION(true, true) THEN WHENRESULT(100, true) "
                + "     WHEN WHENCONDITION(true, false) THEN WHENRESULT(200, false) "
                + "     ELSE WHENRESULT(300, false) "
                + "END "
                + "FROM codegen_test;", metaStore)
            .getSelectExpressions()
            .get(0);

        // When:
        final Object result = codeGenRunner
            .buildCodeGenFromParseTree(expression, "Case")
            .evaluate(genericRow(ONE_ROW));

        // Then:
        assertThat(result, is(100));
    }

    @Test
    public void shouldOnlyRunElseIfNoMatchInWhen() {
        // Given:
        final Expression expression = analyzeQuery(
            "SELECT CASE "
                + "     WHEN WHENCONDITION(false, true) THEN WHENRESULT(100, false) "
                + "     WHEN WHENCONDITION(false, true) THEN WHENRESULT(200, false) "
                + "     ELSE WHENRESULT(300, true) "
                + "END "
                + "FROM codegen_test;", metaStore)
            .getSelectExpressions()
            .get(0);

        // When:
        final Object result = codeGenRunner
            .buildCodeGenFromParseTree(expression, "Case")
            .evaluate(genericRow(ONE_ROW));

        // Then:
        assertThat(result, is(300));
    }

    @Test
    public void shouldReturnDefaultForCaseCorrectly() {
        // Given:
        final Expression expression = analyzeQuery(
            "SELECT CASE "
                + "     WHEN col0 > 10 THEN 'small' "
                + "     ELSE 'large' "
                + "END "
                + "FROM codegen_test;", metaStore)
            .getSelectExpressions()
            .get(0);

        // When:
        final Object result = codeGenRunner
            .buildCodeGenFromParseTree(expression, "Case")
            .evaluate(genericRow(ONE_ROW));

        // Then:
        assertThat(result, is("large"));
    }

    @Test
    public void shouldReturnNullForCaseIfNoDefault() {
        // Given:
        final Expression expression = analyzeQuery(
            "SELECT CASE "
                + "     WHEN col0 > 10 THEN 'small' "
                + "END "
                + "FROM codegen_test;", metaStore)
            .getSelectExpressions()
            .get(0);

        // When:
        final Object result = codeGenRunner
            .buildCodeGenFromParseTree(expression, "Case")
            .evaluate(genericRow(ONE_ROW));

        // Then:
        assertThat(result, is(nullValue()));
    }


    @Test
    public void shouldHandleUdfsExtractingFromMaps() {
        // Given:
        final Expression expression = analyzeQuery(
            "SELECT EXTRACTJSONFIELD(col11['address'], '$.city') FROM codegen_test;",
            metaStore)
            .getSelectExpressions()
            .get(0);

        // When:
        final Object result = codeGenRunner
            .buildCodeGenFromParseTree(expression, "Select")
            .evaluate(genericRow(ONE_ROW));

        // Then:
        assertThat(result, is("adelaide"));
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
    public void shouldHandleFunctionWithVarargs() {
        final String query =
            "SELECT test_udf(col0, col0, col0, col0, col0) FROM codegen_test;";

        final Map<Integer, Object> inputValues = ImmutableMap.of(0, 0);
        final List<Object> columns = executeExpression(query, inputValues);
        // test
        assertThat(columns, equalTo(Collections.singletonList("doStuffLongVarargs")));
    }

    @Test
    public void shouldHandleFunctionWithStruct() {
        // Given:
        final String query =
            "SELECT test_udf(col" + STRUCT_INDEX + ") FROM codegen_test;";

        // When:
        final List<Object> columns = executeExpression(query, ImmutableMap.of());

        // Then:
        assertThat(columns, equalTo(Collections.singletonList("VALUE")));
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
        final Analysis analysis = analyzeQuery(query, metaStore);

        final GenericRow input = buildRow(inputValues);

        return analysis.getSelectExpressions().stream()
            .map(exp -> codeGenRunner.buildCodeGenFromParseTree(exp, "Select"))
            .map(md -> md.evaluate(input))
            .collect(Collectors.toList());
    }

    private boolean evalBooleanExprEq(final int cola, final int colb, final Object[] values) {
        return evalBooleanExpr("SELECT col%d = col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprNeq(final int cola, final int colb, final Object[] values) {
        return evalBooleanExpr("SELECT col%d != col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprIsDistinctFrom(final int cola, final int colb,
        final Object[] values) {
        return evalBooleanExpr("SELECT col%d IS DISTINCT FROM col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprLessThan(final int cola, final int colb, final Object[] values) {
        return evalBooleanExpr("SELECT col%d < col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprLessThanEq(final int cola, final int colb,
        final Object[] values) {
        return evalBooleanExpr("SELECT col%d <= col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprGreaterThan(final int cola, final int colb,
        final Object[] values) {
        return evalBooleanExpr("SELECT col%d > col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExprGreaterThanEq(final int cola, final int colb,
        final Object[] values) {
        return evalBooleanExpr("SELECT col%d >= col%d FROM CODEGEN_TEST;", cola, colb, values);
    }

    private boolean evalBooleanExpr(
        final String queryFormat, final int cola, final int colb, final Object[] values) {
        final String simpleQuery = String.format(queryFormat, cola, colb);
        final Analysis analysis = analyzeQuery(simpleQuery, metaStore);

        final ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getSelectExpressions().get(0), "Filter");
        assertThat(expressionEvaluatorMetadata0.getIndexes(), containsInAnyOrder(cola, colb));
        assertThat(expressionEvaluatorMetadata0.getUdfs(), hasSize(2));

        final List<Object> columns = new ArrayList<>(ONE_ROW);
        columns.set(cola, values[0]);
        columns.set(colb, values[1]);

        final Object result0 = expressionEvaluatorMetadata0.evaluate(genericRow(columns));
        assertThat(result0, instanceOf(Boolean.class));
        return (Boolean)result0;
    }

    private boolean evalBetweenClauseScalar(final int col, final Number val, final Number min, final Number max) {
        final String simpleQuery = String.format("SELECT * FROM CODEGEN_TEST WHERE col%d BETWEEN %s AND %s;", col, min.toString(), max.toString());
        return evalBetweenClause(simpleQuery, col, val);
    }

    private boolean evalNotBetweenClauseScalar(final int col, final Number val, final Number min, final Number max) {
        final String simpleQuery = String.format("SELECT * FROM CODEGEN_TEST WHERE col%d NOT BETWEEN %s AND %s;", col, min.toString(), max.toString());
        return evalBetweenClause(simpleQuery, col, val);
    }

    private boolean evalBetweenClauseString(final int col, final String val, final String min, final String max) {
        final String simpleQuery = String.format("SELECT * FROM CODEGEN_TEST WHERE col%d BETWEEN %s AND %s;", col, min, max);
        return evalBetweenClause(simpleQuery, col, val);
    }

    private boolean evalNotBetweenClauseString(final int col, final String val, final String min, final String max) {
        final String simpleQuery = String.format("SELECT * FROM CODEGEN_TEST WHERE col%d NOT BETWEEN %s AND %s;", col, min, max);
        return evalBetweenClause(simpleQuery, col, val);
    }

    private void evalNotBetweenClauseObject(final int col, final Object val, final String min, final String max) {
        final String simpleQuery = String.format("SELECT * FROM CODEGEN_TEST WHERE col%d NOT BETWEEN %s AND %s;", col, min, max);
        evalBetweenClause(simpleQuery, col, val);
    }

    private boolean evalBetweenClause(final String simpleQuery, final int col, final Object val) {
        final Analysis analysis = analyzeQuery(simpleQuery, metaStore);

        final ExpressionMetadata expressionEvaluatorMetadata0 = codeGenRunner.buildCodeGenFromParseTree
            (analysis.getWhereExpression(), "Filter");

        final List<Object> columns = new ArrayList<>(ONE_ROW);
        columns.set(col, val);

        final Object result0 = expressionEvaluatorMetadata0.evaluate(genericRow(columns));
        assertThat(result0, instanceOf(Boolean.class));
        return (Boolean)result0;
    }

    private static GenericRow buildRow(final Map<Integer, Object> overrides) {
        final List<Object> columns = new ArrayList<>(ONE_ROW);
        overrides.forEach(columns::set);
        return genericRow(columns);
    }

    private static GenericRow genericRow(final Object... columns) {
        return genericRow(Arrays.asList(columns));
    }

    private static GenericRow genericRow(final List<Object> columns) {
        return new GenericRow(columns);
    }

    public static final class WhenCondition implements Kudf {

        @Override
        public Object evaluate(final Object... args) {
            final boolean shouldBeEvaluated = (boolean) args[1];
            if (!shouldBeEvaluated) {
                throw new KsqlException("When condition in case is not running lazily!");
            }
            return args[0];
        }
    }

    public static final class WhenResult implements Kudf {
        @Override
        public Object evaluate(final Object... args) {
            final boolean shouldBeEvaluated = (boolean) args[1];
            if (!shouldBeEvaluated) {
                throw new KsqlException("Then expression in case is not running lazily!");
            }
            return args[0];
        }
    }
}
