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
import static io.confluent.ksql.name.SourceName.of;
import static java.util.Optional.empty;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression.Sign;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.ksql.util.KsqlConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SqlToJavaVisitorTest {

  private static final SourceName TEST1 = SourceName.of("TEST1");

  @Mock
  private FunctionRegistry functionRegistry;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private SqlToJavaVisitor sqlToJavaVisitor;
  private KsqlConfig ksqlConfig;

  @Before
  public void init() {
    final AtomicInteger funCounter = new AtomicInteger();
    final AtomicInteger structCounter = new AtomicInteger();
    ksqlConfig = new KsqlConfig(Collections.emptyMap());
    sqlToJavaVisitor = new SqlToJavaVisitor(
        SCHEMA,
        functionRegistry,
        ref -> ref.text().replace(".", "_"),
        name -> name.text() + "_" + funCounter.getAndIncrement(),
        struct -> "schema" + structCounter.getAndIncrement(),
        ksqlConfig
    );
  }

  @Test
  public void shouldProcessBasicJavaMath() {
    // Given:
    final Expression expression = new ArithmeticBinaryExpression(Operator.ADD, COL0, COL3);

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("(COL0 + COL3)"));
  }

  @Test
  public void shouldProcessArrayExpressionCorrectly() {
    // Given:
    final Expression expression = new SubscriptExpression(ARRAYCOL, literal(0));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        javaExpression,
        equalTo("((Double) (ArrayAccess.arrayAccess((java.util.List) COL4, ((int) 0))))")
    );
  }

  @Test
  public void shouldProcessMapExpressionCorrectly() {
    // Given:
    final Expression expression = new SubscriptExpression(MAPCOL, new StringLiteral("key1"));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("((Double) ((java.util.Map)COL5).get(\"key1\"))"));
  }

  @Test
  public void shouldProcessCreateArrayExpressionCorrectly() {
    // Given:
    Expression expression = new CreateArrayExpression(
        ImmutableList.of(
            new SubscriptExpression(MAPCOL, new StringLiteral("key1")),
            new DoubleLiteral(1.0d)
        )
    );

    // When:
    String java = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        java,
        equalTo("((List)new ArrayBuilder(2).add(((Double) ((java.util.Map)COL5).get(\"key1\"))).add(1E0).build())"));
  }

  @Test
  public void shouldProcessCreateMapExpressionCorrectly() {
    // Given:
    Expression expression = new CreateMapExpression(
        ImmutableMap.of(
            new StringLiteral("foo"),
            new SubscriptExpression(MAPCOL, new StringLiteral("key1")),
            new StringLiteral("bar"),
            new DoubleLiteral(1.0d)
        )
    );

    // When:
    String java = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(java, equalTo("((Map)new MapBuilder(2).put(\"foo\", ((Double) ((java.util.Map)COL5).get(\"key1\"))).put(\"bar\", 1E0).build())"));
  }

  @Test
  public void shouldProcessStructExpressionCorrectly() {
    // Given:
    final Expression expression = new CreateStructExpression(
        ImmutableList.of(
            new Field("col1", new StringLiteral("foo")),
            new Field("col2", new SubscriptExpression(MAPCOL, new StringLiteral("key1")))
        )
    );

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        javaExpression,
        equalTo("((Struct)new Struct(schema0).put(\"col1\",\"foo\").put(\"col2\",((Double) ((java.util.Map)COL5).get(\"key1\"))))"));
  }

  @Test
  public void shouldCreateCorrectCastJavaExpression() {
    // Given:
    final Expression castBigintInteger = new Cast(
        COL0,
        new io.confluent.ksql.execution.expression.tree.Type(SqlPrimitiveType.of("INTEGER"))
    );
    final Expression castDoubleBigint = new Cast(
        COL3,
        new io.confluent.ksql.execution.expression.tree.Type(SqlPrimitiveType.of("BIGINT"))
    );
    final Expression castDoubleString = new Cast(
        COL3,
        new io.confluent.ksql.execution.expression.tree.Type(SqlPrimitiveType.of("VARCHAR"))
    );

    // Then:
    assertThat(
        sqlToJavaVisitor.process(castBigintInteger),
        equalTo("(new Long(COL0).intValue())")
    );
    assertThat(
        sqlToJavaVisitor.process(castDoubleBigint),
        equalTo("(new Double(COL3).longValue())")
    );
    assertThat(
        sqlToJavaVisitor.process(castDoubleString),
        equalTo("Objects.toString(COL3, null)")
    );
  }

  @Test
  public void shouldUseStringValueOfIfConfigSet() {
    // Given:
    final Expression castDoubleString = new Cast(
        COL3,
        new io.confluent.ksql.execution.expression.tree.Type(SqlPrimitiveType.of("VARCHAR"))
    );
    ksqlConfig = new KsqlConfig(Collections.singletonMap(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE, false));
    final AtomicInteger funCounter = new AtomicInteger();
    final AtomicInteger structCounter = new AtomicInteger();
    sqlToJavaVisitor = new SqlToJavaVisitor(
        SCHEMA,
        functionRegistry,
        ref -> ref.text().replace(".", "_"),
        name -> name.text() + "_" + funCounter.getAndIncrement(),
        struct -> "schema" + structCounter.getAndIncrement(),
        ksqlConfig
    );
    
    // Then:
    assertThat(
        sqlToJavaVisitor.process(castDoubleString),
        equalTo("String.valueOf(COL3)")
    );
  }

  @Test
  public void shouldPostfixFunctionInstancesWithUniqueId() {
    // Given:
    final UdfFactory ssFactory = mock(UdfFactory.class);
    final KsqlScalarFunction ssFunction = mock(KsqlScalarFunction.class);
    final UdfFactory catFactory = mock(UdfFactory.class);
    final KsqlScalarFunction catFunction = mock(KsqlScalarFunction.class);
    givenUdf("SUBSTRING", ssFactory, ssFunction);
    when(ssFunction.parameters())
        .thenReturn(ImmutableList.of(ParamTypes.STRING, ParamTypes.INTEGER, ParamTypes.INTEGER));
    givenUdf("CONCAT", catFactory, catFunction);
    when(catFunction.parameters())
        .thenReturn(ImmutableList.of(ParamTypes.STRING, ParamTypes.STRING));
    final FunctionName ssName = FunctionName.of("SUBSTRING");
    final FunctionName catName = FunctionName.of("CONCAT");
    final FunctionCall substring1 = new FunctionCall(
        ssName,
        ImmutableList.of(COL1, new IntegerLiteral(1), new IntegerLiteral(3))
    );
    final FunctionCall substring2 = new FunctionCall(
        ssName,
        ImmutableList.of(COL1, new IntegerLiteral(4), new IntegerLiteral(5))
    );
    final FunctionCall concat = new FunctionCall(
        catName,
        ImmutableList.of(new StringLiteral("-"), substring2)
    );
    final Expression expression = new FunctionCall(
        catName,
        ImmutableList.of(substring1, concat)
    );

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, is(
        "((String) CONCAT_0.evaluate("
            + "((String) SUBSTRING_1.evaluate(COL1, 1, 3)), "
            + "((String) CONCAT_2.evaluate(\"-\","
            + " ((String) SUBSTRING_3.evaluate(COL1, 4, 5))))))"));
  }

  @Test
  public void shouldImplicitlyCastFunctionCallParameters() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    givenUdf("FOO", udfFactory, udf);
    when(udf.parameters()).thenReturn(ImmutableList.of(ParamTypes.DOUBLE, ParamTypes.LONG));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(
        new FunctionCall(
            FunctionName.of("FOO"),
            ImmutableList.of(new DecimalLiteral(new BigDecimal("1.2")), new IntegerLiteral(1))
        )
    );

    // Then:
    assertThat(javaExpression, is(
        "((String) FOO_0.evaluate(((new BigDecimal(\"1.2\")).doubleValue()), (new Integer(1).longValue())))"
    ));
  }

  @Test
  public void shouldImplicitlyCastFunctionCallParametersVariadic() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    givenUdf("FOO", udfFactory, udf);
    when(udf.parameters()).thenReturn(ImmutableList.of(ParamTypes.DOUBLE, ArrayType.of(ParamTypes.LONG)));
    when(udf.isVariadic()).thenReturn(true);

    // When:
    final String javaExpression = sqlToJavaVisitor.process(
        new FunctionCall(
            FunctionName.of("FOO"),
            ImmutableList.of(
                new DecimalLiteral(new BigDecimal("1.2")),
                new IntegerLiteral(1),
                new IntegerLiteral(1))
        )
    );

    // Then:
    assertThat(javaExpression, is(
        "((String) FOO_0.evaluate(((new BigDecimal(\"1.2\")).doubleValue()), (new Integer(1).longValue()), (new Integer(1).longValue())))"
    ));
  }

  @Test
  public void shouldHandleFunctionCallsWithGenerics() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    givenUdf("FOO", udfFactory, udf);
    when(udf.parameters()).thenReturn(ImmutableList.of(GenericType.of("T"), GenericType.of("T")));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(
        new FunctionCall(
            FunctionName.of("FOO"),
            ImmutableList.of(
                new IntegerLiteral(1),
                new IntegerLiteral(1))
        )
    );

    // Then:
    assertThat(javaExpression, is("((String) FOO_0.evaluate(1, 1))"));
  }

  @Test
  public void shouldEscapeQuotesInStringLiteral() {
    // Given:
    final Expression expression = new StringLiteral("\"foo\"");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("\"\\\"foo\\\"\""));
  }

  @Test
  public void shouldEscapeQuotesInStringLiteralQuote() {
    // Given:
    final Expression expression = new StringLiteral("\\\"");

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("\"\\\\\\\"\""));
  }

  @Test
  public void shouldGenerateCorrectCodeForComparisonWithNegativeNumbers() {
    // Given:
    final Expression expression = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        COL3,
        new DoubleLiteral(-10.0)
    );

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(
        javaExpression, equalTo(
            "((((Object)(COL3)) == null || ((Object)(-1E1)) == null) ? false : (COL3 > -1E1))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePattern() {
    // Given:
    final Expression expression = new LikePredicate(COL1, new StringLiteral("%foo"), Optional.empty());

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("LikeEvaluator.matches(COL1, \"%foo\")"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithEscape() {
    // Given:
    final Expression expression = new LikePredicate(COL1, new StringLiteral("%foo"), Optional.of('!'));

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("LikeEvaluator.matches(COL1, \"%foo\", '!')"));
  }

  @Test
  public void shouldGenerateCorrectCodeForLikePatternWithColRef() {
    // Given:
    final Expression expression = new LikePredicate(COL1, COL1, Optional.empty());

    // When:
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // Then:
    assertThat(javaExpression, equalTo("LikeEvaluator.matches(COL1, COL1)"));
  }

  @Test
  public void shouldGenerateCorrectCodeForCaseStatement() {
    // Given:
    final Expression expression = new SearchedCaseExpression(
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
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // ThenL
    assertThat(
        javaExpression, equalTo(
            "((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.of( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(10)) == null) ? false : (COL7 < 10)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(100)) == null) ? false : (COL7 < 100)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }})), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"large\"; }}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForCaseStatementWithNoElse() {
    // Given:
    final Expression expression = new SearchedCaseExpression(
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
    final String javaExpression = sqlToJavaVisitor.process(expression);

    // ThenL
    assertThat(
        javaExpression, equalTo(
            "((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.of( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(10)) == null) ? false : (COL7 < 10)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(100)) == null) ? false : (COL7 < 100)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }})), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return null; }}))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalAdd() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(COL8.add(COL8, new MathContext(3, RoundingMode.UNNECESSARY)).setScale(1))")
    );
  }

  @Test
  public void shouldGenerateCastLongToDecimalInBinaryExpression() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL0"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, containsString("DecimalUtil.cast(COL0, 19, 0)"));
  }

  @Test
  public void shouldGenerateCastDecimalToDoubleInBinaryExpression() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.ADD,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL3"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, containsString("(COL8).doubleValue()"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalSubtract() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.SUBTRACT,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(COL8.subtract(COL8, new MathContext(3, RoundingMode.UNNECESSARY)).setScale(1))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalMultiply() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.MULTIPLY,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(COL8.multiply(COL8, new MathContext(5, RoundingMode.UNNECESSARY)).setScale(2))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDivide() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.DIVIDE,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(COL8.divide(COL8, new MathContext(8, RoundingMode.UNNECESSARY)).setScale(6))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalMod() {
    // Given:
    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
        Operator.MODULUS,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(
        java,
        is("(COL8.remainder(COL8, new MathContext(2, RoundingMode.UNNECESSARY)).setScale(1))")
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(COL8.compareTo(COL9) == 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalGT() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(COL8.compareTo(COL9) > 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalGEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.GREATER_THAN_OR_EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(COL8.compareTo(COL9) >= 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalLT() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(COL8.compareTo(COL9) < 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalLEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.LESS_THAN_OR_EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(COL8.compareTo(COL9) <= 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDecimalIsDistinct() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.IS_DISTINCT_FROM,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(COL8.compareTo(COL9) != 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalDoubleEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL3"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(COL8.compareTo(BigDecimal.valueOf(COL3)) == 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDoubleDecimalEQ() {
    // Given:
    final ComparisonExpression compExp = new ComparisonExpression(
        ComparisonExpression.Type.EQUAL,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL3")),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(compExp);

    // Then:
    assertThat(java, containsString("(BigDecimal.valueOf(COL3).compareTo(COL8) == 0))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalNegation() {
    // Given:
    final ArithmeticUnaryExpression binExp = new ArithmeticUnaryExpression(
        Optional.empty(),
        Sign.MINUS,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(COL8.negate(new MathContext(2, RoundingMode.UNNECESSARY)))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalUnaryPlus() {
    // Given:
    final ArithmeticUnaryExpression binExp = new ArithmeticUnaryExpression(
        Optional.empty(),
        Sign.PLUS,
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
    );

    // When:
    final String java = sqlToJavaVisitor.process(binExp);

    // Then:
    assertThat(java, is("(COL8.plus(new MathContext(2, RoundingMode.UNNECESSARY)))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalCast() {
    // Given:
    final Cast cast = new Cast(
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL3")),
        new Type(SqlDecimal.of(2, 1))
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("(DecimalUtil.cast(COL3, 2, 1))"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalCastNoOp() {
    // Given:
    final Cast cast = new Cast(
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new Type(SqlDecimal.of(2, 1))
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("COL8"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToIntCast() {
    // Given:
    final Cast cast = new Cast(
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new Type(SqlTypes.INTEGER)
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("((COL8).intValue())"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToLongCast() {
    // Given:
    final Cast cast = new Cast(
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new Type(SqlTypes.BIGINT)
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("((COL8).longValue())"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToDoubleCast() {
    // Given:
    final Cast cast = new Cast(
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new Type(SqlTypes.DOUBLE)
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("((COL8).doubleValue())"));
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToStringCast() {
    // Given:
    final Cast cast = new Cast(
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
        new Type(SqlTypes.STRING)
    );

    // When:
    final String java = sqlToJavaVisitor.process(cast);

    // Then:
    assertThat(java, is("DecimalUtil.format(2, 1, COL8)"));
  }

  @Test
  public void shouldThrowOnQualifiedColumnReference() {
    // Given:
    final Expression expression = new QualifiedColumnReferenceExp(
        of("foo"),
        ColumnName.of("bar")
    );

    // When:
    assertThrows(
        UnsupportedOperationException.class,
        () -> sqlToJavaVisitor.process(expression)
    );
  }

  @Test
  public void shouldThrowOnIn() {
    // Given:
    final Expression expression = new InPredicate(
        COL0,
        new InListExpression(ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)))
    );

    // When:
    assertThrows(
        UnsupportedOperationException.class,
        () -> sqlToJavaVisitor.process(expression)
    );
  }

  @Test
  public void shouldThrowOnSimpleCase() {
    // Given:
    final Expression expression = new SimpleCaseExpression(
        COL0,
        ImmutableList.of(new WhenClause(new IntegerLiteral(10), new StringLiteral("ten"))),
        empty()
    );

    // When:
    assertThrows(
        UnsupportedOperationException.class,
        () -> sqlToJavaVisitor.process(expression)
    );
  }

  @Test
  public void shouldThrowOnTimeLiteral() {
    // When:
    assertThrows(
        UnsupportedOperationException.class,
        () -> sqlToJavaVisitor.process(new TimeLiteral("TIME '00:00:00'"))
    );
  }

  @Test
  public void shouldThrowOnTimestampLiteral() {
    // When:
    assertThrows(
        UnsupportedOperationException.class,
        () -> sqlToJavaVisitor.process(new TimestampLiteral("TIMESTAMP '00:00:00'"))
    );
  }

  private void givenUdf(
      final String name, final UdfFactory factory, final KsqlScalarFunction function
  ) {
    when(functionRegistry.isAggregate(FunctionName.of(name))).thenReturn(false);
    when(functionRegistry.getUdfFactory(FunctionName.of(name))).thenReturn(factory);
    when(factory.getFunction(anyList())).thenReturn(function);
    when(function.getReturnType(anyList())).thenReturn(SqlTypes.STRING);
    final UdfMetadata metadata = mock(UdfMetadata.class);
    when(factory.getMetadata()).thenReturn(metadata);
  }
}
