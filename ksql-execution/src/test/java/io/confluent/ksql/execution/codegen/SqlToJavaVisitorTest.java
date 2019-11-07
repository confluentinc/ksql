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

package io.confluent.ksql.execution.codegen;

import static io.confluent.ksql.execution.testutil.TestExpressions.ARRAYCOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL0;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL1;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL3;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL7;
import static io.confluent.ksql.execution.testutil.TestExpressions.MAPCOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.SCHEMA;
import static io.confluent.ksql.execution.testutil.TestExpressions.literal;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression.Sign;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SqlToJavaVisitorTest {

  private static final SourceName TEST1 = SourceName.of("TEST1");

  @Mock
  private FunctionRegistry functionRegistry;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private SqlToJavaVisitor sqlToJavaVisitor;

  @Before
  public void init() {
    AtomicInteger funCounter = new AtomicInteger();
    sqlToJavaVisitor = new SqlToJavaVisitor(
        SCHEMA,
        functionRegistry,
        ref -> ref.aliasedFieldName().replace(".", "_"),
        name -> name.name() + "_" + funCounter.getAndIncrement()
    );
  }

  @Test
  public void shouldProcessBasicJavaMath() {
    // Given:
    Expression expression = new ArithmeticBinaryExpression(Operator.ADD, COL0, COL3);

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(TEST1_COL0 + TEST1_COL3)"));
  }

  @Test
  public void shouldProcessArrayExpressionCorrectly() {
    // Given:
    Expression expression = new SubscriptExpression(ARRAYCOL, literal(0));

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        javaExpression,
        equalTo("((Double) ((java.util.List)TEST1_COL4).get((int)0))")
    );
  }

  @Test
  public void shouldProcessArrayNegativeIndexExpressionCorrectly() {
    // Given:
    Expression expression = new SubscriptExpression(
        ARRAYCOL,
        ArithmeticUnaryExpression.negative(Optional.empty(), new IntegerLiteral(1))
    );

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        javaExpression,
        equalTo(
            "((Double) ((java.util.List)TEST1_COL4).get((int)((java.util.List)TEST1_COL4).size()-1))")
    );
  }

  @Test
  public void shouldProcessMapExpressionCorrectly() {
    // Given:
    Expression expression = new SubscriptExpression(MAPCOL, new StringLiteral("key1"));

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("((Double) ((java.util.Map)TEST1_COL5).get(\"key1\"))"));
  }

  @Test
  public void shouldCreateCorrectCastJavaExpression() {
    // Given:
    Expression castBigintInteger = new Cast(
        COL0,
        new io.confluent.ksql.execution.expression.tree.Type(SqlPrimitiveType.of("INTEGER"))
    );
    Expression castDoubleBigint = new Cast(
        COL3,
        new io.confluent.ksql.execution.expression.tree.Type(SqlPrimitiveType.of("BIGINT"))
    );
    Expression castDoubleString = new Cast(
        COL3,
        new io.confluent.ksql.execution.expression.tree.Type(SqlPrimitiveType.of("VARCHAR"))
    );

    // Then:
    assertThat(
        sqlToJavaVisitor.process(castBigintInteger),
        equalTo("(new Long(TEST1_COL0).intValue())")
    );
    assertThat(
        sqlToJavaVisitor.process(castDoubleBigint),
        equalTo("(new Double(TEST1_COL3).longValue())")
    );
    assertThat(
        sqlToJavaVisitor.process(castDoubleString),
        equalTo("String.valueOf(TEST1_COL3)")
    );
  }

  @Test
  public void shouldPostfixFunctionInstancesWithUniqueId() {
    // Given:
    UdfFactory ssFactory = mock(UdfFactory.class);
    KsqlScalarFunction ssFunction = mock(KsqlScalarFunction.class);
    UdfFactory catFactory = mock(UdfFactory.class);
    KsqlScalarFunction catFunction = mock(KsqlScalarFunction.class);
    givenUdf("SUBSTRING", Schema.OPTIONAL_STRING_SCHEMA, ssFactory, ssFunction);
    givenUdf("CONCAT", Schema.OPTIONAL_STRING_SCHEMA, catFactory, catFunction);
    FunctionName ssName = FunctionName.of("SUBSTRING");
    FunctionName catName = FunctionName.of("CONCAT");
    FunctionCall substring1 = new FunctionCall(
        ssName,
        ImmutableList.of(COL1, new IntegerLiteral(1), new IntegerLiteral(3))
    );
    FunctionCall substring2 = new FunctionCall(
        ssName,
        ImmutableList.of(COL1, new IntegerLiteral(4), new IntegerLiteral(5))
    );
    FunctionCall concat = new FunctionCall(
        catName,
        ImmutableList.of(new StringLiteral("-"), substring2)
    );
    Expression expression = new FunctionCall(
        catName,
        ImmutableList.of(substring1, concat)
    );

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

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
    Expression expression = new StringLiteral("'\"foo\"'");

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("\"\\\"foo\\\"\""));
  }

  @Test
  public void shouldEscapeQuotesInStringLiteralQuote() {
    // Given:
    Expression expression = new StringLiteral("'\\\"'");

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("\"\\\\\\\"\""));
  }

  @Test
  public void shouldGenerateCorrectCodeForComparisonWithNegativeNumbers() {
    // Given:
    Expression expression = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        COL3,
        new DoubleLiteral(-10.0)
    );

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        javaExpression, equalTo(
            "((((Object)(TEST1_COL3)) == null || ((Object)(-10.0)) == null) ? false : (TEST1_COL3 > -10.0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithLeadingWildcard() {
    // Given:
    Expression expression = new LikePredicate(COL1, new StringLiteral("%foo"));

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(TEST1_COL1).endsWith(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithTrailingWildcard() {
    // Given:
    Expression expression = new LikePredicate(COL1, new StringLiteral("foo%"));

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(TEST1_COL1).startsWith(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithLeadingAndTrailingWildcards() {
    // Given:
    Expression expression = new LikePredicate(COL1, new StringLiteral("%foo%"));

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(TEST1_COL1).contains(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithoutWildcards() {
    // Given:
    Expression expression = new LikePredicate(COL1, new StringLiteral("foo"));

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(TEST1_COL1).equals(\"foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForCaseStatement() {
    // Given:
    Expression expression = new SearchedCaseExpression(
        ImmutableList.of(
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(10)),
                new StringLiteral("small")
            ),
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(100)),
                new StringLiteral("medium")
            )
        ),
        Optional.of(new StringLiteral("large"))
    );

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // ThenL
    assertThat(
        javaExpression, equalTo(
            "((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.of( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(10)) == null) ? false : (TEST1_COL7 < 10)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(100)) == null) ? false : (TEST1_COL7 < 100)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }})), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"large\"; }}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForCaseStatementWithNoElse() {
    // Given:
    Expression expression = new SearchedCaseExpression(
        ImmutableList.of(
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(10)),
                new StringLiteral("small")
            ),
            new WhenClause(
                new ComparisonExpression(
                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(100)),
                new StringLiteral("medium")
            )
        ),
        Optional.empty()
    );

    // When:
    String javaExpression = sqlToJavaVisitor.process(expression);

    // ThenL
    assertThat(
        javaExpression, equalTo(
            "((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.of( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(10)) == null) ? false : (TEST1_COL7 < 10)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(TEST1_COL7)) == null || ((Object)(100)) == null) ? false : (TEST1_COL7 < 100)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }})), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return null; }}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalAdd() {
    // Given:
    ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8")))
    );

    // When:
    String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(TEST1_COL8.add(TEST1_COL8, new MathContext(3, RoundingMode.UNNECESSARY)).setScale(1))")
    );
  }

  @Test
  public void shouldGenerateCastLongToDecimalInBinaryExpression() {
    // Given:
    ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL0")))
    );

    // When:
    String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, containsString("DecimalUtil.cast(TEST1_COL0, 19, 0)"));
  }

  @Test
  public void shouldGenerateCastDecimalToDoubleInBinaryExpression() {
    // Given:
    ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL3")))
    );

    // When:
    String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8).doubleValue()"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalSubtract() {
    // Given:
    ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.SUBTRACT,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8")))
    );

    // When:
    String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(TEST1_COL8.subtract(TEST1_COL8, new MathContext(3, RoundingMode.UNNECESSARY)).setScale(1))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalMultiply() {
    // Given:
    ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.MULTIPLY,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8")))
    );

    // When:
    String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(TEST1_COL8.multiply(TEST1_COL8, new MathContext(5, RoundingMode.UNNECESSARY)).setScale(2))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDivide() {
    // Given:
    ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.DIVIDE,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8")))
    );

    // When:
    String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(TEST1_COL8.divide(TEST1_COL8, new MathContext(8, RoundingMode.UNNECESSARY)).setScale(6))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalMod() {
    // Given:
    ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.MODULUS,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8")))
    );

    // When:
    String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(TEST1_COL8.remainder(TEST1_COL8, new MathContext(2, RoundingMode.UNNECESSARY)).setScale(1))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalEQ() {
    // Given:
    ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL9")))
    );

    // When:
    String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) == 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalGT() {
    // Given:
    ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL9")))
    );

    // When:
    String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) > 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalGEQ() {
    // Given:
    ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN_OR_EQUAL,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL9")))
    );

    // When:
    String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) >= 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalLT() {
    // Given:
    ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL9")))
    );

    // When:
    String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) < 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalLEQ() {
    // Given:
    ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN_OR_EQUAL,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL9")))
    );

    // When:
    String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) <= 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalIsDistinct() {
    // Given:
    ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.IS_DISTINCT_FROM,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL9")))
    );

    // When:
    String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(TEST1_COL9) != 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDoubleEQ() {
    // Given:
    ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL3")))
    );

    // When:
    String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(TEST1_COL8.compareTo(new BigDecimal(TEST1_COL3)) == 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDoubleDecimalEQ() {
    // Given:
    ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL3"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8")))
    );

    // When:
    String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(new BigDecimal(TEST1_COL3).compareTo(TEST1_COL8) == 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalNegation() {
    // Given:
    ArithmeticUnaryExpression binExp = new ArithmeticUnaryExpression(
        Optional.empty(),
        Sign.MINUS,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8")))
    );

    // When:
    String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(TEST1_COL8.negate(new MathContext(2, RoundingMode.UNNECESSARY)))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalUnaryPlus() {
    // Given:
    ArithmeticUnaryExpression binExp = new ArithmeticUnaryExpression(
        Optional.empty(),
        Sign.PLUS,
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8")))
    );

    // When:
    String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(TEST1_COL8.plus(new MathContext(2, RoundingMode.UNNECESSARY)))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalCast() {
    // Given:
    Cast cast = new Cast(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL3"))),
        new Type(SqlDecimal.of(2, 1))
    );

    // When:
    String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("(DecimalUtil.cast(TEST1_COL3, 2, 1))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalCastNoOp() {
    // Given:
    Cast cast = new Cast(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new Type(SqlDecimal.of(2, 1))
    );

    // When:
    String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("TEST1_COL8"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToIntCast() {
    // Given:
    Cast cast = new Cast(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new Type(SqlTypes.INTEGER)
    );

    // When:
    String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("((TEST1_COL8).intValue())"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToLongCast() {
    // Given:
    Cast cast = new Cast(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new Type(SqlTypes.BIGINT)
    );

    // When:
    String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("((TEST1_COL8).longValue())"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToDoubleCast() {
    // Given:
    Cast cast = new Cast(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new Type(SqlTypes.DOUBLE)
    );

    // When:
    String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("((TEST1_COL8).doubleValue())"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToStringCast() {
    // Given:
    Cast cast = new Cast(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL8"))),
        new Type(SqlTypes.STRING)
    );

    // When:
    String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("DecimalUtil.format(2, 1, TEST1_COL8)"));
  }

  @Test
  public void shouldThrowOnIn() {
    // Given:
    Expression expression = new InPredicate(
        COL0,
        new InListExpression(ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)))
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(expression);
  }

  @Test
  public void shouldThrowOnSimpleCase() {
    // Given:
    Expression expression = new SimpleCaseExpression(
        COL0,
        ImmutableList.of(new WhenClause(new IntegerLiteral(10), new StringLiteral("ten"))),
        Optional.empty()
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(expression);
  }

  @Test
  public void shouldThrowOnTimeLiteral() {
    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(new TimeLiteral("TIME '00:00:00'"));
  }

  @Test
  public void shouldThrowOnTimestampLiteral() {
    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    sqlToJavaVisitor.process(new TimestampLiteral("TIMESTAMP '00:00:00'"));
  }

  private void givenUdf(
      String name, Schema returnType, UdfFactory factory, KsqlScalarFunction function
  ) {
    when(functionRegistry.isAggregate(name)).thenReturn(false);
    when(functionRegistry.getUdfFactory(name)).thenReturn(factory);
    when(factory.getFunction(anyList())).thenReturn(function);
    when(function.getReturnType(anyList())).thenReturn(returnType);
    UdfMetadata metadata = mock(UdfMetadata.class);
    when(factory.getMetadata()).thenReturn(metadata);
  }
}
