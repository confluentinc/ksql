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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression.Sign;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Optional;
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
        .field("TEST1.COL8", DecimalUtil.builder(2, 1).build())
        .field("TEST1.COL9", DecimalUtil.builder(2, 1).build())
        .build();

    sqlToJavaVisitor = new SqlToJavaVisitor(LogicalSchema.of(schema), TestFunctionRegistry.INSTANCE.get());
  }

  @Test
  public void shouldProcessBasicJavaMath() {
    // Given:
    final Expression expression = parseExpression("col0+col3");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(TEST1_COL0 + TEST1_COL3)"));
  }

  @Test
  public void shouldProcessArrayExpressionCorrectly() {
    // Given:
    final Expression expression = parseExpression("col4[0]");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression,
       equalTo("((Double) ((java.util.List)TEST1_COL4).get((int)0))"));
  }

  @Test
  public void shouldProcessArrayNegativeIndexExpressionCorrectly() {
    // Given:
    final Expression expression = parseExpression("col4[-1]");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression,
            equalTo("((Double) ((java.util.List)TEST1_COL4).get((int)((java.util.List)TEST1_COL4).size()-1))"));
  }

  @Test
  public void shouldProcessMapExpressionCorrectly() {
    // Given:
    final Expression expression = parseExpression("col5['key1']");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("((Double) ((java.util.Map)TEST1_COL5).get(\"key1\"))"));
  }

  @Test
  public void shouldCreateCorrectCastJavaExpression() {
    // Given/When:
    final String javaExpression0 = sqlToJavaVisitor.process(
        parseExpression("cast(col0 AS INTEGER)"));
    final String javaExpression1 = sqlToJavaVisitor.process(
        parseExpression("cast(col3 as BIGINT)"));
    final String javaExpression2 = sqlToJavaVisitor.process(
        parseExpression("cast(col3 as varchar)"));

    // Then:
    assertThat(javaExpression0, equalTo("(new Long(TEST1_COL0).intValue())"));
    assertThat(javaExpression1, equalTo("(new Double(TEST1_COL3).longValue())"));
    assertThat(javaExpression2, equalTo("String.valueOf(TEST1_COL3)"));
  }

  @Test
  public void shouldPostfixFunctionInstancesWithUniqueId() {
    // Given:
    final Expression expression = parseExpression(
        "CONCAT(SUBSTRING(col1,1,3),CONCAT('-',SUBSTRING(col1,4,5)))");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, is(
        "((String) CONCAT_0.evaluate("
            + "((String) SUBSTRING_1.evaluate(TEST1_COL1, 1, 3)), "
            + "((String) CONCAT_2.evaluate(\"-\","
            + " ((String) SUBSTRING_3.evaluate(TEST1_COL1, 4, 5))))))"));
  }

  @Test
  public void shouldEscapeQuotesInStringLiteral() {
    // Given:
    final Expression expression = parseExpression("'\"foo\"'");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("\"\\\"foo\\\"\""));
  }

  @Test
  public void shouldEscapeQuotesInStringLiteralQuote() {
    // Given:
    final Expression expression = parseExpression("'\\\"'");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("\"\\\\\\\"\""));
  }

  @Test
  public void shouldGenerateCorrectCodeForComparisonWithNegativeNumbers() {
    // Given:
    final Expression expression = parseExpression("col3 > -10.0");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("((((Object)(TEST1_COL3)) == null || ((Object)(-10.0)) == null) ? false : (TEST1_COL3 > -10.0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithLeadingWildcard() {
    // Given:
    final Expression expression = parseExpression("col1 LIKE '%foo'");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(TEST1_COL1).endsWith(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithTrailingWildcard() {
    // Given:
    final Expression expression = parseExpression(" col1 LIKE 'foo%'");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(TEST1_COL1).startsWith(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithLeadingAndTrailingWildcards() {
    // Given:
    final Expression expression = parseExpression("col1 LIKE '%foo%'");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(TEST1_COL1).contains(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithoutWildcards() {
    // Given:
    final Expression expression = parseExpression("col1 LIKE 'foo'");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(TEST1_COL1).equals(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForCaseStatement() {
    // Given:
    final Expression expression = parseExpression(
        "CASE"
            + "    WHEN col7 < 10 THEN 'small' "
            + "    WHEN col7 < 100 THEN 'medium' "
            + "    ELSE 'large'"
            + "  END ");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // ThenL
    assertThat(javaExpression, equalTo("((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.of( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(10)) == null) ? false : (TEST1_COL7 < 10)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(100)) == null) ? false : (TEST1_COL7 < 100)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }})), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"large\"; }}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForCaseStatementWithNoElse() {
    // Given:
    final Expression expression = parseExpression(
        "CASE"
            + "     WHEN col7 < 10 THEN 'small' "
            + "     WHEN col7 < 100 THEN 'medium' "
            + "  END ");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // ThenL
    assertThat(javaExpression, equalTo("((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.of( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(10)) == null) ? false : (TEST1_COL7 < 10)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(100)) == null) ? false : (TEST1_COL7 < 100)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }})), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return null; }}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalAdd() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(TEST1_COL8.add(TEST1_COL8, new MathContext(3, RoundingMode.UNNECESSARY)).setScale(1))"));
  }

  @Test
  public void shouldGenerateCastLongToDecimalInBinaryExpression() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL0"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, containsString("DecimalUtil.cast(TEST1_COL0, 19, 0)"));
  }

  @Test
  public void shouldGenerateCastDecimalToDoubleInBinaryExpression() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL3"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8).doubleValue()"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalSubtract() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.SUBTRACT,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(TEST1_COL8.subtract(TEST1_COL8, new MathContext(3, RoundingMode.UNNECESSARY)).setScale(1))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalMultiply() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.MULTIPLY,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(TEST1_COL8.multiply(TEST1_COL8, new MathContext(5, RoundingMode.UNNECESSARY)).setScale(2))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDivide() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.DIVIDE,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(TEST1_COL8.divide(TEST1_COL8, new MathContext(8, RoundingMode.UNNECESSARY)).setScale(6))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalMod() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.MODULUS,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(TEST1_COL8.remainder(TEST1_COL8, new MathContext(2, RoundingMode.UNNECESSARY)).setScale(1))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) == 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalGT() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) > 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalGEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN_OR_EQUAL,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) >= 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalLT() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) < 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalLEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN_OR_EQUAL,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) <= 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalIsDistinct() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.IS_DISTINCT_FROM,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) != 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDoubleEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL3"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(new BigDecimal(TEST1_COL3)) == 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDoubleDecimalEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL3")),
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(new BigDecimal(TEST1_COL3).compareTo(TEST1_COL8) == 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalNegation() {
    // Given:
    final ArithmeticUnaryExpression binExp = new ArithmeticUnaryExpression(
        Optional.empty(),
        Sign.MINUS,
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(TEST1_COL8.negate(new MathContext(2, RoundingMode.UNNECESSARY)))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalUnaryPlus() {
    // Given:
    final ArithmeticUnaryExpression binExp = new ArithmeticUnaryExpression(
        Optional.empty(),
        Sign.PLUS,
    new QualifiedNameReference(QualifiedName.of("TEST1.COL8")));

      // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(TEST1_COL8.plus(new MathContext(2, RoundingMode.UNNECESSARY)))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalCast() {
    // Given:
    final Cast cast = new Cast(
        new QualifiedNameReference(QualifiedName.of("TEST1.COL3")),
        new Type(SqlDecimal.of(2, 1))
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("(DecimalUtil.cast(TEST1_COL3, 2, 1))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalCastNoOp() {
    // Given:
    final Cast cast = new Cast(
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new Type(SqlDecimal.of(2, 1))
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("TEST1_COL8"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToIntCast() {
    // Given:
    final Cast cast = new Cast(
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new Type(SqlTypes.INTEGER)
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("((TEST1_COL8).intValue())"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToLongCast() {
    // Given:
    final Cast cast = new Cast(
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new Type(SqlTypes.BIGINT)
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("((TEST1_COL8).longValue())"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToDoubleCast() {
    // Given:
    final Cast cast = new Cast(
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new Type(SqlTypes.DOUBLE)
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("((TEST1_COL8).doubleValue())"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToStringCast() {
    // Given:
    final Cast cast = new Cast(
        new QualifiedNameReference(QualifiedName.of("TEST1.COL8")),
        new Type(SqlTypes.STRING)
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("DecimalUtil.format(2, 1, TEST1_COL8)"));
  }

  @Test
  public void shouldThrowOnIn() {
    // Given:
    final Expression expression = parseExpression("col1 IN (1,2,3)");

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(expression);
  }

  @Test
  public void shouldThrowOnNotIn() {
    // Given:
    final Expression expression = parseExpression("col1 NOT IN (1,2,3)");

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(expression);
  }

  @Test
  public void shouldThrowOnSimpleCase() {
    // Given:
    final Expression expression = parseExpression(
        "CASE col7 "
            + "WHEN 10 THEN 'ten' "
            + "WHEN 100 THEN 'one hundred' "
            + "END"
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(expression);
  }

  @Test
  public void shouldThrowOnTimeLiteral() {
    // Given:
    final Expression expression = parseExpression("TIME '12:34:56.789'");

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(expression);
  }

  @Test
  public void shouldThrowOnTimestampLiteral() {
    // Given:
    final Expression expression = parseExpression("TIMESTAMP '1776-07-04 12:34:56.789'");

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(expression);
  }

  private Expression parseExpression(final String asText) {
    final KsqlParser parser = new DefaultKsqlParser();
    final String ksql = String.format("SELECT %s FROM test1;", asText);

    final ParsedStatement parsedStatement = parser.parse(ksql).get(0);
    final PreparedStatement preparedStatement = parser.prepare(parsedStatement, metaStore);
    final SingleColumn singleColumn = (SingleColumn) ((Query)preparedStatement.getStatement())
        .getSelect()
        .getSelectItems()
        .get(0);
    return singleColumn.getExpression();
  }
}
