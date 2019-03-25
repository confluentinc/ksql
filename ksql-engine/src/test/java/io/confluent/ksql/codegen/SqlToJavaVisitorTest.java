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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.MetaStoreFixture;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SqlToJavaVisitorTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private MetaStore metaStore;
  private SqlToJavaVisitor sqlToJavaVisitor;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(TestFunctionRegistry.INSTANCE.get());

    final Schema addressSchema = SchemaBuilder.struct()
        .field("NUMBER",Schema.OPTIONAL_INT64_SCHEMA)
        .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
        .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
        .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
        .optional().build();

    final Schema schema = SchemaBuilder.struct()
        .field("TEST1.COL0", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field("TEST1.COL1", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("TEST1.COL2", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("TEST1.COL3", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
        .field("TEST1.COL4", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("TEST1.COL5", SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("TEST1.COL6", addressSchema)
        .field("TEST1.COL7", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .build();

    sqlToJavaVisitor = new SqlToJavaVisitor(schema, TestFunctionRegistry.INSTANCE.get());
  }

  @Test
  public void shouldProcessBasicJavaMath() {
    final String simpleQuery = "SELECT col0+col3, col2, col3+10, col0*25, 12*4+2 FROM test1 WHERE col0 > 100;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);

    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getSelectExpressions().get(0));

    assertThat(javaExpression, equalTo("(TEST1_COL0 + TEST1_COL3)"));
  }

  @Test
  public void shouldProcessArrayExpressionCorrectly() {
    final String simpleQuery = "SELECT col4[0] FROM test1 WHERE col0 > 100;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);

    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getSelectExpressions().get(0));

    assertThat(javaExpression,
        equalTo("((Double) ((java.util.List)TEST1_COL4).get((int)(Integer.parseInt(\"0\"))))"));
  }

  @Test
  public void shouldProcessMapExpressionCorrectly() {
    final String simpleQuery = "SELECT col5['key1'] FROM test1 WHERE col0 > 100;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);

    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getSelectExpressions().get(0));

    assertThat(javaExpression, equalTo("((Double) ((java.util.Map)TEST1_COL5).get(\"key1\"))"));
  }

  @Test
  public void shouldCreateCorrectCastJavaExpression() {

    final String simpleQuery = "SELECT cast(col0 AS INTEGER), cast(col3 as BIGINT), cast(col3 as "
        + "varchar) FROM "
        + "test1 WHERE "
        + "col0 > 100;";
    final Analysis analysis = analyzeQuery(simpleQuery, metaStore);

    final String javaExpression0 = sqlToJavaVisitor
        .process(analysis.getSelectExpressions().get(0));
    final String javaExpression1 = sqlToJavaVisitor
        .process(analysis.getSelectExpressions().get(1));
    final String javaExpression2 = sqlToJavaVisitor
        .process(analysis.getSelectExpressions().get(2));

    assertThat(javaExpression0, equalTo("(new Long(TEST1_COL0).intValue())"));
    assertThat(javaExpression1, equalTo("(new Double(TEST1_COL3).longValue())"));
    assertThat(javaExpression2, equalTo("String.valueOf(TEST1_COL3)"));
  }

  @Test
  public void shouldPostfixFunctionInstancesWithUniqueId() {
    final Analysis analysis = analyzeQuery(
        "SELECT CONCAT(SUBSTRING(col1,1,3),CONCAT('-',SUBSTRING(col1,4,5))) FROM test1;",
        metaStore);

    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getSelectExpressions().get(0));

    assertThat(javaExpression, is(
        "((String) CONCAT_0.evaluate("
            + "((String) SUBSTRING_1.evaluate(TEST1_COL1, Integer.parseInt(\"1\"), Integer.parseInt(\"3\"))), "
            + "((String) CONCAT_2.evaluate(\"-\","
            + " ((String) SUBSTRING_3.evaluate(TEST1_COL1, Integer.parseInt(\"4\"), Integer.parseInt(\"5\")))))))"));
  }

  @Test
  public void shouldEscapeQuotesInStringLiteral() {
    final Analysis analysis = analyzeQuery(
        "SELECT '\"foo\"' FROM test1;", metaStore);

    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getSelectExpressions().get(0));
    assertThat(javaExpression, equalTo("\"\\\"foo\\\"\""));
  }

  @Test
  public void shouldEscapeQuotesInStringLiteralQuote() {
    final Analysis analysis = analyzeQuery(
        "SELECT '\\\"' FROM test1;", metaStore);
    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getSelectExpressions().get(0));
    assertThat(javaExpression, equalTo("\"\\\\\\\"\""));
  }

  @Test
  public void shouldGenerateCorrectCodeForComparisonWithNegativeNumbers() {
    final Analysis analysis = analyzeQuery(
        "SELECT * FROM test1 WHERE col3 > -10.0;", metaStore);

    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getWhereExpression());
    assertThat(javaExpression, equalTo("((((Object)(TEST1_COL3)) == null || ((Object)(-10.0)) == null) ? false : (TEST1_COL3 > -10.0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithLeadingWildcard() {
    final Analysis analysis = analyzeQuery(
        "SELECT * FROM test1 WHERE col1 LIKE '%foo';", metaStore);

    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getWhereExpression());
    assertThat(javaExpression, equalTo("(TEST1_COL1).endsWith(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithTrailingWildcard() {
    final Analysis analysis = analyzeQuery(
        "SELECT * FROM test1 WHERE col1 LIKE 'foo%';", metaStore);

    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getWhereExpression());
    assertThat(javaExpression, equalTo("(TEST1_COL1).startsWith(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithLeadingAndTrailingWildcards() {
    final Analysis analysis = analyzeQuery(
        "SELECT * FROM test1 WHERE col1 LIKE '%foo%';", metaStore);

    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getWhereExpression());
    assertThat(javaExpression, equalTo("(TEST1_COL1).contains(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithoutWildcards() {
    final Analysis analysis = analyzeQuery(
        "SELECT * FROM test1 WHERE col1 LIKE 'foo';", metaStore);

    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getWhereExpression());
    assertThat(javaExpression, equalTo("(TEST1_COL1).equals(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForCaseStatement() {
    // Given:
    final Analysis analysis = analyzeQuery(
        "SELECT CASE"
            + "    WHEN col7 < 10 THEN 'small' "
            + "    WHEN col7 < 100 THEN 'medium' "
            + "    ELSE 'large'"
            + "  END "
            + "FROM test1;", metaStore);

    // When:
    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getSelectExpressions().get(0));

    // ThenL
    assertThat(javaExpression, equalTo("((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.of( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(Integer.parseInt(\"10\"))) == null) ? false : (TEST1_COL7 < Integer.parseInt(\"10\"))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(Integer.parseInt(\"100\"))) == null) ? false : (TEST1_COL7 < Integer.parseInt(\"100\"))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }})), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"large\"; }}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForCaseStatementWithNoElse() {
    // Given:
    final Analysis analysis = analyzeQuery(
        "SELECT CASE"
            + "     WHEN col7 < 10 THEN 'small' "
            + "     WHEN col7 < 100 THEN 'medium' "
            + "  END "
            + "FROM test1;", metaStore);

    // When:
    final String javaExpression = sqlToJavaVisitor
        .process(analysis.getSelectExpressions().get(0));

    // ThenL
    assertThat(javaExpression, equalTo("((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.of( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(Integer.parseInt(\"10\"))) == null) ? false : (TEST1_COL7 < Integer.parseInt(\"10\"))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(Integer.parseInt(\"100\"))) == null) ? false : (TEST1_COL7 < Integer.parseInt(\"100\"))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }})), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return null; }}))"));
  }

  @Test
  public void shouldThrowOnDecimal() {
    // Given:
    final Analysis analysis = analyzeQuery(
        "SELECT DECIMAL 'something' FROM test1;", metaStore);

    final Expression decimal = analysis.getSelectExpressions().get(0);

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(decimal);
  }

  @Test
  public void shouldThrowOnIn() {
    // Given:
    final Analysis analysis = analyzeQuery(
        "SELECT * FROM test1 WHERE col1 IN (1,2,3);", metaStore);

    final Expression where = analysis.getWhereExpression();

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(where);
  }

  @Test
  public void shouldThrowOnNotIn() {
    // Given:
    final Analysis analysis = analyzeQuery(
        "SELECT * FROM test1 WHERE col1 NOT IN (1,2,3);", metaStore);

    final Expression decimal = analysis.getWhereExpression();

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(decimal);
  }
}