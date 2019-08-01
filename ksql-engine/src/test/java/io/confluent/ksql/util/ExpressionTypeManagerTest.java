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

package io.confluent.ksql.util;

import static io.confluent.ksql.testutils.AnalysisTestUtil.analyzeQuery;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.testutils.ExpressionParseTestUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class ExpressionTypeManagerTest {

  private static final FunctionRegistry FUNCTION_REGISTRY = TestFunctionRegistry.INSTANCE.get();

  private MetaStore metaStore;
  private ExpressionTypeManager expressionTypeManager;
  private ExpressionTypeManager ordersExpressionTypeManager;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(FUNCTION_REGISTRY);

    final Schema schema = SchemaBuilder.struct()
        .field("TEST1.COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("TEST1.COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("TEST1.COL2", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("TEST1.COL3", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    expressionTypeManager = new ExpressionTypeManager(LogicalSchema.of(schema), FUNCTION_REGISTRY);
    ordersExpressionTypeManager = new ExpressionTypeManager(
        metaStore.getSource("ORDERS").getSchema(),
        FUNCTION_REGISTRY
    );
  }

  @Test
  public void testArithmeticExpr() {
    final String simpleQuery = "SELECT col0+col3, col2, col3+10, col0+10, col0*25 FROM test1 WHERE col0 > 100;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);
    final Schema exprType0 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
    final Schema exprType2 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(2));
    final Schema exprType3 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(3));
    final Schema exprType4 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(4));
    Assert.assertTrue(exprType0.type() == Schema.Type.FLOAT64);
    Assert.assertTrue(exprType2.type() == Schema.Type.FLOAT64);
    Assert.assertTrue(exprType3.type() == Schema.Type.INT64);
    Assert.assertTrue(exprType4.type() == Schema.Type.INT64);
  }

  @Test
  public void testComparisonExpr() {
    final String simpleQuery = "SELECT col0>col3, col0*25<200, col2 = 'test' FROM test1;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);
    final Schema exprType0 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
    final Schema exprType1 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1));
    final Schema exprType2 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(2));
    Assert.assertTrue(exprType0.type() == Schema.Type.BOOLEAN);
    Assert.assertTrue(exprType1.type() == Schema.Type.BOOLEAN);
    Assert.assertTrue(exprType2.type() == Schema.Type.BOOLEAN);
  }

  @Test
  public void shouldFailIfComparisonOperandsAreIncompatible() {
    // Given:
    final String simpleQuery = "SELECT col1 > 10 FROM test1;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Operator GREATER_THAN cannot be used to compare STRING and INTEGER");

    // When:
    expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));

  }

  @Test
  public void shouldFailIfOperatorCannotBeAppiled() {
    // Given:
    final String simpleQuery = "SELECT true > false FROM test1;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Operator GREATER_THAN cannot be used to compare BOOLEAN");

    // When:
    expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));

  }

  @Test
  public void shouldFailForComplexTypeComparison() {
    // Given:
    final Analysis analysis = analyzeQuery("SELECT MAPCOL > NESTED_ORDER_COL from NESTED_STREAM;", metaStore);
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        metaStore.getSource("NESTED_STREAM").getSchema(),
        FUNCTION_REGISTRY
    );
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Operator GREATER_THAN cannot be used to compare MAP and STRUCT");

    // When:
    expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
  }

  @Test
  public void shouldFailForComparingComplexTypes() {
    // Given:
    final Analysis analysis = analyzeQuery("SELECT NESTED_ORDER_COL = NESTED_ORDER_COL from NESTED_STREAM;", metaStore);
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        metaStore.getSource("NESTED_STREAM").getSchema(),
        FUNCTION_REGISTRY
    );
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Operator EQUAL cannot be used to compare STRUCT and STRUCT");

    // When:
    expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));

  }

  @Test
  public void shouldEvaluateBooleanSchemaForLikeExpression() {
    final String simpleQuery = "SELECT col1 LIKE 'foo%', col2 LIKE '%bar' FROM test1;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);

    final Schema exprType0 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
    final Schema exprType1 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1));
    Assert.assertTrue(exprType0.type() == Schema.Type.BOOLEAN);
    Assert.assertTrue(exprType1.type() == Schema.Type.BOOLEAN);
  }

  @Test
  public void shouldEvaluateBooleanSchemaForNotLikeExpression() {
    final String simpleQuery = "SELECT col1 NOT LIKE 'foo%', col2 NOT LIKE '%bar' FROM test1;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);

    final Schema exprType0 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
    final Schema exprType1 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1));
    Assert.assertTrue(exprType0.type() == Schema.Type.BOOLEAN);
    Assert.assertTrue(exprType1.type() == Schema.Type.BOOLEAN);
  }

  @Test
  public void testUDFExpr() {
    final String simpleQuery = "SELECT FLOOR(col3), CEIL(col3*3), ABS(col0+1.34), RANDOM()+10, ROUND(col3*2)+12 FROM test1;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);
    final Schema exprType0 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
    final Schema exprType1 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1));
    final Schema exprType2 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(2));
    final Schema exprType3 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(3));
    final Schema exprType4 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(4));

    Assert.assertTrue(exprType0.type() == Schema.Type.FLOAT64);
    Assert.assertTrue(exprType1.type() == Schema.Type.FLOAT64);
    Assert.assertTrue(exprType2.type() == Schema.Type.FLOAT64);
    Assert.assertTrue(exprType3.type() == Schema.Type.FLOAT64);
    Assert.assertTrue(exprType4.type() == Schema.Type.INT64);
  }

  @Test
  public void testStringUDFExpr() {
    final String simpleQuery = "SELECT LCASE(col1), UCASE(col2), TRIM(col1), CONCAT(col1,'_test'), SUBSTRING(col1, 1, 3) FROM test1;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);
    final Schema exprType0 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
    final Schema exprType1 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1));
    final Schema exprType2 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(2));
    final Schema exprType3 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(3));
    final Schema exprType4 = expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(4));

    Assert.assertTrue(exprType0.type() == Schema.Type.STRING);
    Assert.assertTrue(exprType1.type() == Schema.Type.STRING);
    Assert.assertTrue(exprType2.type() == Schema.Type.STRING);
    Assert.assertTrue(exprType3.type() == Schema.Type.STRING);
    Assert.assertTrue(exprType4.type() == Schema.Type.STRING);
  }

  @Test
  public void shouldHandleNestedUdfs() {
    final Analysis analysis = analyzeQuery("SELECT SUBSTRING(EXTRACTJSONFIELD(col1,'$.name'),"
        + "LEN(col1) - 2) FROM test1;", metaStore);

    assertThat(expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0)),
        equalTo(Schema.OPTIONAL_STRING_SCHEMA));

  }

  @Test
  public void shouldHandleStruct() {
    final Analysis analysis = analyzeQuery("SELECT itemid, address->zipcode, address->state from orders;", metaStore);

    assertThat(ordersExpressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0)),
        equalTo(Schema.OPTIONAL_STRING_SCHEMA));

    assertThat(ordersExpressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1)),
        equalTo(Schema.OPTIONAL_INT64_SCHEMA));

    assertThat(ordersExpressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(2)),
        equalTo(Schema.OPTIONAL_STRING_SCHEMA));

  }

  @Test
  public void shouldFailIfThereIsInvalidFieldNameInStructCall() {
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not find field ZIP in ORDERS.ADDRESS.");
    final Analysis analysis = analyzeQuery(
        "SELECT itemid, address->zip, address->state from orders;", metaStore);
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        metaStore.getSource("ORDERS").getSchema(),
        FUNCTION_REGISTRY
    );
    expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1));
  }

  @Test
  public void shouldFindTheNestedArrayTypeCorrectly() {
    final Analysis analysis = analyzeQuery("SELECT ARRAYCOL[0]->CATEGORY->NAME, NESTED_ORDER_COL->arraycol[0] from NESTED_STREAM;", metaStore);
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        metaStore.getSource("NESTED_STREAM").getSchema(),
        FUNCTION_REGISTRY
    );
    assertThat(expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0)),
        equalTo(Schema.OPTIONAL_STRING_SCHEMA));
    assertThat(expressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(1)),
        equalTo(Schema.OPTIONAL_FLOAT64_SCHEMA));

  }

  @Test
  public void shouldGetCorrectSchemaForSearchedCase() {
    // Given:
    final Analysis analysis = analyzeQuery("SELECT CASE WHEN orderunits < 10 THEN 'small' WHEN orderunits < 100 THEN 'medium' ELSE 'large' END FROM orders;", metaStore);

    // When:
    final Schema caseSchema = ordersExpressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));

    // Then:
    assertThat(caseSchema, equalTo(Schema.OPTIONAL_STRING_SCHEMA));

  }

  @Test
  public void shouldGetCorrectSchemaForSearchedCaseWhenStruct() {
    // Given:
    final Analysis analysis = analyzeQuery("SELECT CASE WHEN orderunits < 10 THEN ADDRESS END FROM orders;", metaStore);

    // When:
    final Schema caseSchema = ordersExpressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));

    // Then:
    final SqlType sqlType = metaStore
        .getSource("ORDERS")
        .getSchema()
        .findValueField("ADDRESS")
        .get()
        .type();

    assertThat(caseSchema, equalTo(SchemaConverters.sqlToConnectConverter().toConnectSchema(sqlType)));
  }

  @Test
  public void shouldFailIfWhenIsNotBoolean() {
    // Given:
    final Analysis analysis = analyzeQuery("SELECT CASE WHEN orderunits < 10 THEN 'small' WHEN orderunits + 100 THEN 'medium' ELSE 'large' END FROM orders;", metaStore);
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("When operand schema should be boolean. Schema for ((ORDERS.ORDERUNITS + 100)) is Schema{INT32}");

    // When:
    ordersExpressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));

  }

  @Test
  public void shouldFailOnInconsistentWhenResultType() {
    // Given:
    final Analysis analysis = analyzeQuery("SELECT CASE WHEN orderunits < 10 THEN 'small' WHEN orderunits < 100 THEN 10 ELSE 'large' END FROM orders;", metaStore);
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid Case expression. Schemas for 'THEN' clauses should be the same. Result schema: Schema{STRING}. Schema for THEN expression 'WHEN (ORDERS.ORDERUNITS < 100) THEN 10' is Schema{INT32}");

    // When:
    ordersExpressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));

  }

  @Test
  public void shouldFailIfDefaultHasDifferentTypeToWhen() {
    // Given:
    final Analysis analysis = analyzeQuery("SELECT CASE WHEN orderunits < 10 THEN 'small' WHEN orderunits < 100 THEN 'medium' ELSE true END FROM orders;", metaStore);
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid Case expression. Schema for the default clause should be the same as schema for THEN clauses. Result scheme: Schema{STRING}. Schema for default expression is Schema{BOOLEAN}");

    // When:
    ordersExpressionTypeManager.getExpressionSchema(analysis.getSelectExpressions().get(0));
  }

  @Test
  public void shouldThrowOnTimeLiteral() {
    final Expression expression = ExpressionParseTestUtil.parseExpression(
        "TIME '00:00:00'",
        metaStore
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    ordersExpressionTypeManager.getExpressionSchema(expression);
  }

  @Test
  public void shouldThrowOnTimestampLiteral() {
    final Expression expression = ExpressionParseTestUtil.parseExpression(
        "TIMESTAMP '00:00:00'",
        metaStore
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    ordersExpressionTypeManager.getExpressionSchema(expression);
  }

  @Test
  public void shouldThrowOnIn() {
    final Expression expression = ExpressionParseTestUtil.parseExpression(
        "orderunits IN (1,2,3)",
        metaStore
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    ordersExpressionTypeManager.getExpressionSchema(expression);
  }

  @Test
  public void shouldThrowOnSimpleCase() {
    final Expression expression = ExpressionParseTestUtil.parseExpression(
        "CASE orderunits "
            + "WHEN 10 THEN 'ten' "
            + "WHEN 100 THEN 'one hundred' "
            + "END",
        metaStore
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    ordersExpressionTypeManager.getExpressionSchema(expression);
  }
}
