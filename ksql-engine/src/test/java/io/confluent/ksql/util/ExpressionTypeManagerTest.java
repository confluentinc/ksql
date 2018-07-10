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

package io.confluent.ksql.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.UdfLoaderUtil;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.Statement;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class ExpressionTypeManagerTest {

    private static final KsqlParser KSQL_PARSER = new KsqlParser();
    private MetaStore metaStore;
    private Schema schema;
    private InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void init() {
        metaStore = MetaStoreFixture.getNewMetaStore(functionRegistry);
        // load udfs that are not hardcoded
        UdfLoaderUtil.load(metaStore);
        schema = SchemaBuilder.struct()
                .field("TEST1.COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
                .field("TEST1.COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                .field("TEST1.COL2", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                .field("TEST1.COL3", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA);
    }

    private Analysis analyzeQuery(String queryStr) {
        final List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
        final Analysis analysis = new Analysis();
        final Analyzer analyzer = new Analyzer("sqlExpression", analysis, metaStore, "");
        analyzer.process(statements.get(0), new AnalysisContext(null));
        return analysis;
    }

    @Test
    public void testArithmaticExpr() {
        String simpleQuery = "SELECT col0+col3, col2, col3+10, col0+10, col0*25 FROM test1 WHERE col0 > 100;";
        Analysis analysis = analyzeQuery(simpleQuery);
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema,
                                                                                functionRegistry);
        Schema exprType0 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
        Schema exprType2 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(2));
        Schema exprType3 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(3));
        Schema exprType4 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(4));
        Assert.assertTrue(exprType0.type() == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType2.type() == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType3.type() == Schema.Type.INT64);
        Assert.assertTrue(exprType4.type() == Schema.Type.INT64);
    }

    @Test
    public void testComparisonExpr() {
        String simpleQuery = "SELECT col0>col3, col0*25<200, col2 = 'test' FROM test1;";
        Analysis analysis = analyzeQuery(simpleQuery);
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema,
                                                                                functionRegistry);
        Schema exprType0 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
        Schema exprType1 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1));
        Schema exprType2 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(2));
        Assert.assertTrue(exprType0.type() == Schema.Type.BOOLEAN);
        Assert.assertTrue(exprType1.type() == Schema.Type.BOOLEAN);
        Assert.assertTrue(exprType2.type() == Schema.Type.BOOLEAN);
    }

    @Test
    public void testUDFExpr() {
        String simpleQuery = "SELECT FLOOR(col3), CEIL(col3*3), ABS(col0+1.34), RANDOM()+10, ROUND(col3*2)+12 FROM test1;";
        Analysis analysis = analyzeQuery(simpleQuery);
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema,
                                                                                functionRegistry);
        Schema exprType0 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
        Schema exprType1 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1));
        Schema exprType2 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(2));
        Schema exprType3 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(3));
        Schema exprType4 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(4));

        Assert.assertTrue(exprType0.type() == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType1.type() == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType2.type() == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType3.type() == Schema.Type.FLOAT64);
        Assert.assertTrue(exprType4.type() == Schema.Type.INT64);
    }

    @Test
    public void testStringUDFExpr() {
        String simpleQuery = "SELECT LCASE(col1), UCASE(col2), TRIM(col1), CONCAT(col1,'_test'), SUBSTRING(col1, 1, 3) FROM test1;";
        Analysis analysis = analyzeQuery(simpleQuery);
        ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema,
                                                                                functionRegistry);
        Schema exprType0 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
        Schema exprType1 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1));
        Schema exprType2 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(2));
        Schema exprType3 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(3));
        Schema exprType4 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(4));

        Assert.assertTrue(exprType0.type() == Schema.Type.STRING);
        Assert.assertTrue(exprType1.type() == Schema.Type.STRING);
        Assert.assertTrue(exprType2.type() == Schema.Type.STRING);
        Assert.assertTrue(exprType3.type() == Schema.Type.STRING);
        Assert.assertTrue(exprType4.type() == Schema.Type.STRING);
    }

    @Test
    public void shouldHandleNestedUdfs() {
        final Analysis analysis = analyzeQuery("SELECT SUBSTRING(EXTRACTJSONFIELD(col1,'$.name'),"
            + "LEN(col1) - 2) FROM test1;");

        final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema,
            functionRegistry);

        assertThat(expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0)),
            equalTo(Schema.OPTIONAL_STRING_SCHEMA));

    }

  @Test
  public void shouldHandleStruct() {
    final Analysis analysis = analyzeQuery("SELECT itemid, address->zipcode, address->state from orders;");

    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(metaStore.getSource("ORDERS").getSchema(),
        functionRegistry);

    assertThat(expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0)),
        equalTo(Schema.OPTIONAL_STRING_SCHEMA));

    assertThat(expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1)),
        equalTo(Schema.OPTIONAL_INT64_SCHEMA));

    assertThat(expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(2)),
        equalTo(Schema.OPTIONAL_STRING_SCHEMA));

  }

  @Test
  public void shouldFailIfThereIsInvalidFieldNameInStructCall() {
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not find field ZIP in ORDERS.ADDRESS.");
    final Analysis analysis = analyzeQuery(
        "SELECT itemid, address->zip, address->state from orders;");
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        metaStore.getSource("ORDERS").getSchema(),
        functionRegistry);
    expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1));
  }

  @Test
  public void shouldFindTheNestedArrayTypeCorrectly() {
    final Analysis analysis = analyzeQuery("SELECT ARRAYCOL[0]->CATEGORY->NAME, NESTED_ORDER_COL->arraycol[0] from NESTED_STREAM;");
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(metaStore.getSource("NESTED_STREAM").getSchema(),
        functionRegistry);
    assertThat(expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0)),
        equalTo(Schema.OPTIONAL_STRING_SCHEMA));
    assertThat(expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1)),
        equalTo(Schema.OPTIONAL_FLOAT64_SCHEMA));

  }
}
