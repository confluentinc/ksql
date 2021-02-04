/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.evaluator;

import static io.confluent.ksql.execution.testutil.TestExpressions.COL11;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL3;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL7;
import static io.confluent.ksql.execution.testutil.TestExpressions.SCHEMA;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.IntegerType;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class ExpressionInterpreterTest {

  public final static SqlStruct ADDRESS_SCHEMA2 = SqlTypes.struct()
      .field("NUMBER", SqlTypes.BIGINT)
      .field("STREET", SqlTypes.STRING)
      .field("CITY", SqlTypes.STRING)
      .field("STATE", SqlTypes.STRING)
      .field("ZIPCODE", SqlTypes.BIGINT)
      .build();

  public final static Schema ADDRESS_SCHEMA = SchemaBuilder.struct()
      .field("NUMBER", SchemaBuilder.int64())
      .field("STREET", SchemaBuilder.string())
      .field("CITY", SchemaBuilder.string())
      .field("STATE", SchemaBuilder.string())
      .field("ZIPCODE", SchemaBuilder.int64())
      .schema();

  private Struct ADDRESS = new Struct(ADDRESS_SCHEMA)
      .put("NUMBER", 123L)
      .put("STREET", "Main Street")
      .put("CITY", "Mountain View")
      .put("STATE", "CA")
      .put("ZIPCODE", 90210L);

  private GenericRow ROW = new GenericRow().appendAll(ImmutableList.of(
      15, "A", "b", 4.5, ImmutableList.of(2.5), ImmutableMap.of(123, 6.7), ADDRESS, 35,
      new BigDecimal("3.4"), new BigDecimal("8.9"), new Timestamp(1234), true
  ));

  private GenericRow ROW2 = new GenericRow().appendAll(ImmutableList.of(
      30, "A", "b", -20.8, ImmutableList.of(2.5), ImmutableMap.of(123, 6.7), ADDRESS, 35,
      new BigDecimal("3.4"), new BigDecimal("8.9"), new Timestamp(1234), false
  ));

  @Mock
  private FunctionRegistry functionRegistry;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private KsqlConfig ksqlConfig;

  @Before
  public void init() {
    ksqlConfig = new KsqlConfig(Collections.emptyMap());
  }

  private ExpressionInterpreter interpreter(Expression expression) {
    return new ExpressionInterpreter(
        functionRegistry,
        SCHEMA,
        ksqlConfig,
        expression,
        null
    );
  }

  private GenericRow make(int rowNum, Object val) {
    int i = 0;
    GenericRow row = new GenericRow();
    for (Object o : ROW.values()) {
      if (i == rowNum) {
        row.append(val);
      } else {
        row.append(o);
      }
      i++;
    }
    return row;
  }

  @Test
  public void shouldEvaluateComparisons_double() {
    // Given:
    final Expression expression1 = new ComparisonExpression(
        Type.GREATER_THAN,
        COL3,
        new DoubleLiteral(-10.0)
    );
    final Expression expression2 = new ComparisonExpression(
        Type.LESS_THAN,
        COL3,
        new DoubleLiteral(10)
    );
    final Expression expression3 = new ComparisonExpression(
        Type.EQUAL,
        COL3,
        new DoubleLiteral(6.5)
    );

    // When:
    ExpressionInterpreter interpreter1 = interpreter(expression1);
    ExpressionInterpreter interpreter2 = interpreter(expression2);
    ExpressionInterpreter interpreter3 = interpreter(expression3);

    // Then:
    assertThat(interpreter1.evaluate(make(3, 5)), is(true));
    assertThat(interpreter1.evaluate(make(3, -20)), is(false));
    assertThat(interpreter2.evaluate(make(3, 5)), is(true));
    assertThat(interpreter2.evaluate(make(3, 20)), is(false));
    assertThat(interpreter3.evaluate(make(3, 6.5d)), is(true));
    assertThat(interpreter3.evaluate(make(3, 8.5d)), is(false));
  }

  @Test
  public void shouldEvaluateComparisons_int() {
    // Given:
    final Expression expression1 = new ComparisonExpression(
        Type.GREATER_THAN,
        COL7,
        new IntegerLiteral(10)
    );
    final Expression expression2 = new ComparisonExpression(
        Type.LESS_THAN,
        COL7,
        new IntegerLiteral(20)
    );
    final Expression expression3 = new ComparisonExpression(
        Type.EQUAL,
        COL7,
        new DoubleLiteral(30)
    );

    // When:
    ExpressionInterpreter interpreter1 = interpreter(expression1);
    ExpressionInterpreter interpreter2 = interpreter(expression2);
    ExpressionInterpreter interpreter3 = interpreter(expression3);

    // Then:
    assertThat(interpreter1.evaluate(make(7, 30)), is(true));
    assertThat(interpreter1.evaluate(make(7, 4)), is(false));
    assertThat(interpreter2.evaluate(make(7, 13)), is(true));
    assertThat(interpreter2.evaluate(make(7, 20)), is(false));
    assertThat(interpreter3.evaluate(make(7, 30)), is(true));
    assertThat(interpreter3.evaluate(make(7, 31)), is(false));
  }

  @Test
  public void shouldEvaluateLogicalExpressions_and() {
    // Given:
    final Expression expression1 = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        COL11,
        new BooleanLiteral(true)
    );
    final Expression expression2 = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.AND,
        COL11,
        new BooleanLiteral(false)
    );

    // When:
    ExpressionInterpreter interpreter1 = interpreter(expression1);
    ExpressionInterpreter interpreter2 = interpreter(expression2);

    // Then:
    assertThat(interpreter1.evaluate(make(11, true)), is(true));
    assertThat(interpreter1.evaluate(make(11, false)), is(false));
    assertThat(interpreter2.evaluate(make(11, true)), is(false));
    assertThat(interpreter2.evaluate(make(11, false)), is(false));
  }

  @Test
  public void shouldEvaluateLogicalExpressions_or() {
    // Given:
    final Expression expression1 = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.OR,
        COL11,
        new BooleanLiteral(true)
    );
    final Expression expression2 = new LogicalBinaryExpression(
        LogicalBinaryExpression.Type.OR,
        COL11,
        new BooleanLiteral(false)
    );

    // When:
    ExpressionInterpreter interpreter1 = interpreter(expression1);
    ExpressionInterpreter interpreter2 = interpreter(expression2);

    // Then:
    assertThat(interpreter1.evaluate(make(11, true)), is(true));
    assertThat(interpreter1.evaluate(make(11, false)), is(true));
    assertThat(interpreter2.evaluate(make(11, true)), is(true));
    assertThat(interpreter2.evaluate(make(11, false)), is(false));
  }

  @Test
  public void shouldHandleFunctionCallsWithGenerics() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    when(udf.newInstance(any())).thenReturn(new AddUdf());
    givenUdf("FOO", udfFactory, udf);
    when(udf.parameters()).thenReturn(ImmutableList.of(GenericType.of("T"), GenericType.of("T")));

    // When:
    ExpressionInterpreter interpreter1 = interpreter(
        new FunctionCall(
            FunctionName.of("FOO"),
            ImmutableList.of(
                new IntegerLiteral(1),
                new IntegerLiteral(1))
        )
    );
    final Object object = interpreter1.evaluate(ROW);

    // Then:
    assertThat(object, is(2));
  }

  @Test
  public void shouldHandleFunctionCalls_intParams() {
    // Given:
    final UdfFactory udfFactory = mock(UdfFactory.class);
    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
    when(udf.newInstance(any())).thenReturn(new AddUdf());
    givenUdf("FOO", udfFactory, udf);
    when(udf.parameters()).thenReturn(ImmutableList.of(IntegerType.INSTANCE, IntegerType.INSTANCE));

    // When:
    ExpressionInterpreter interpreter1 = interpreter(
        new FunctionCall(
            FunctionName.of("FOO"),
            ImmutableList.of(
                new IntegerLiteral(1),
                new IntegerLiteral(1))
        )
    );
    final Object object = interpreter1.evaluate(ROW);


    // Then:
    assertThat(object, is(2));
  }

  @Test
  public void shouldEvaluateIsNullPredicate() {
    // Given:
    final Expression expression1 = new IsNullPredicate(
        COL11
    );
    final Expression expression2 = new IsNullPredicate(
        new NullLiteral()
    );

    // When:
    ExpressionInterpreter interpreter1 = interpreter(expression1);
    ExpressionInterpreter interpreter2 = interpreter(expression2);

    // Then:
    assertThat(interpreter1.evaluate(make(11, true)), is(false));
    assertThat(interpreter1.evaluate(make(11, null)), is(true));
    assertThat(interpreter2.evaluate(ROW), is(true));
  }

  @Test
  public void shouldEvaluateIsNotNullPredicate() {
    // Given:
    final Expression expression1 = new IsNotNullPredicate(
        COL11
    );
    final Expression expression2 = new IsNotNullPredicate(
        new NullLiteral()
    );

    // When:
    ExpressionInterpreter interpreter1 = interpreter(expression1);
    ExpressionInterpreter interpreter2 = interpreter(expression2);

    // Then:
    assertThat(interpreter1.evaluate(make(11, true)), is(true));
    assertThat(interpreter1.evaluate(make(11, null)), is(false));
    assertThat(interpreter2.evaluate(ROW), is(false));
  }

//  @Test
//  public void shouldProcessBasicJavaMath() {
//    // Given:
//    final Expression expression = new ArithmeticBinaryExpression(Operator.ADD, COL0, COL3);
//
//    // When:
//    final Object object = expressionInterpreter.evaluate(expression, ROW);
//
//    // Then:
//    assertThat(object, equalTo(19.5d));
//  }
//
//  @Test
//  public void shouldProcessArrayExpressionCorrectly() {
//    // Given:
//    final Expression expression = new SubscriptExpression(ARRAYCOL, literal(0));
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(
//        javaExpression,
//        equalTo("((Double) (ArrayAccess.arrayAccess((java.util.List) COL4, ((int) 0))))")
//    );
//  }
//
//  @Test
//  public void shouldProcessMapExpressionCorrectly() {
//    // Given:
//    final Expression expression = new SubscriptExpression(MAPCOL, new StringLiteral("key1"));
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(javaExpression, equalTo("((Double) ((java.util.Map)COL5).get(\"key1\"))"));
//  }
//
//  @Test
//  public void shouldProcessCreateArrayExpressionCorrectly() {
//    // Given:
//    Expression expression = new CreateArrayExpression(
//        ImmutableList.of(
//            new SubscriptExpression(MAPCOL, new StringLiteral("key1")),
//            new DoubleLiteral(1.0d)
//        )
//    );
//
//    // When:
//    String java = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(
//        java,
//        equalTo("((List)new ArrayBuilder(2).add(((Double) ((java.util.Map)COL5).get(\"key1\"))).add(1E0).build())"));
//  }
//
//  @Test
//  public void shouldProcessCreateMapExpressionCorrectly() {
//    // Given:
//    Expression expression = new CreateMapExpression(
//        ImmutableMap.of(
//            new StringLiteral("foo"),
//            new SubscriptExpression(MAPCOL, new StringLiteral("key1")),
//            new StringLiteral("bar"),
//            new DoubleLiteral(1.0d)
//        )
//    );
//
//    // When:
//    String java = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(java, equalTo("((Map)new MapBuilder(2).put(\"foo\", ((Double) ((java.util.Map)COL5).get(\"key1\"))).put(\"bar\", 1E0).build())"));
//  }
//
//  @Test
//  public void shouldProcessStructExpressionCorrectly() {
//    // Given:
//    final Expression expression = new CreateStructExpression(
//        ImmutableList.of(
//            new Field("col1", new StringLiteral("foo")),
//            new Field("col2", new SubscriptExpression(MAPCOL, new StringLiteral("key1")))
//        )
//    );
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(
//        javaExpression,
//        equalTo("((Struct)new Struct(schema0).put(\"col1\",\"foo\").put(\"col2\",((Double) ((java.util.Map)COL5).get(\"key1\"))))"));
//  }
//
//  @Test
//  public void shouldCreateCorrectCastJavaExpression() {
//    // Given:
//    final Expression castBigintInteger = new Cast(
//        COL0,
//        new io.confluent.ksql.execution.expression.tree.Type(SqlPrimitiveType.of("INTEGER"))
//    );
//
//    // When:
//    final String actual = sqlToJavaVisitor.process(castBigintInteger);
//
//    // Then:
//    final String expected = CastEvaluator
//        .generateCode("COL0", SqlTypes.BIGINT, SqlTypes.INTEGER, ksqlConfig);
//
//    assertThat(actual, is(expected));
//  }
//
//  @Test
//  public void shouldPostfixFunctionInstancesWithUniqueId() {
//    // Given:
//    final UdfFactory ssFactory = mock(UdfFactory.class);
//    final KsqlScalarFunction ssFunction = mock(KsqlScalarFunction.class);
//    final UdfFactory catFactory = mock(UdfFactory.class);
//    final KsqlScalarFunction catFunction = mock(KsqlScalarFunction.class);
//    givenUdf("SUBSTRING", ssFactory, ssFunction);
//    when(ssFunction.parameters())
//        .thenReturn(ImmutableList.of(ParamTypes.STRING, ParamTypes.INTEGER, ParamTypes.INTEGER));
//    givenUdf("CONCAT", catFactory, catFunction);
//    when(catFunction.parameters())
//        .thenReturn(ImmutableList.of(ParamTypes.STRING, ParamTypes.STRING));
//    final FunctionName ssName = FunctionName.of("SUBSTRING");
//    final FunctionName catName = FunctionName.of("CONCAT");
//    final FunctionCall substring1 = new FunctionCall(
//        ssName,
//        ImmutableList.of(COL1, new IntegerLiteral(1), new IntegerLiteral(3))
//    );
//    final FunctionCall substring2 = new FunctionCall(
//        ssName,
//        ImmutableList.of(COL1, new IntegerLiteral(4), new IntegerLiteral(5))
//    );
//    final FunctionCall concat = new FunctionCall(
//        catName,
//        ImmutableList.of(new StringLiteral("-"), substring2)
//    );
//    final Expression expression = new FunctionCall(
//        catName,
//        ImmutableList.of(substring1, concat)
//    );
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(javaExpression, is(
//        "((String) CONCAT_0.evaluate("
//            + "((String) SUBSTRING_1.evaluate(COL1, 1, 3)), "
//            + "((String) CONCAT_2.evaluate(\"-\","
//            + " ((String) SUBSTRING_3.evaluate(COL1, 4, 5))))))"));
//  }
//
//  @Test
//  public void shouldImplicitlyCastFunctionCallParameters() {
//    // Given:
//    final UdfFactory udfFactory = mock(UdfFactory.class);
//    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
//    givenUdf("FOO", udfFactory, udf);
//    when(udf.parameters()).thenReturn(ImmutableList.of(ParamTypes.DOUBLE, ParamTypes.LONG));
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(
//        new FunctionCall(
//            FunctionName.of("FOO"),
//            ImmutableList.of(new DecimalLiteral(new BigDecimal("1.2")), new IntegerLiteral(1))
//        )
//    );
//
//    // Then:
//    final String doubleCast = CastEvaluator.generateCode(
//        "new BigDecimal(\"1.2\")", SqlTypes.decimal(2, 1), SqlTypes.DOUBLE, ksqlConfig);
//
//    final String longCast = CastEvaluator.generateCode(
//        "1", SqlTypes.INTEGER, SqlTypes.BIGINT, ksqlConfig);
//
//    assertThat(javaExpression, is(
//        "((String) FOO_0.evaluate(" +doubleCast + ", " + longCast + "))"
//    ));
//  }
//
//  @Test
//  public void shouldImplicitlyCastFunctionCallParametersVariadic() {
//    // Given:
//    final UdfFactory udfFactory = mock(UdfFactory.class);
//    final KsqlScalarFunction udf = mock(KsqlScalarFunction.class);
//    givenUdf("FOO", udfFactory, udf);
//    when(udf.parameters()).thenReturn(ImmutableList.of(ParamTypes.DOUBLE, ArrayType.of(ParamTypes.LONG)));
//    when(udf.isVariadic()).thenReturn(true);
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(
//        new FunctionCall(
//            FunctionName.of("FOO"),
//            ImmutableList.of(
//                new DecimalLiteral(new BigDecimal("1.2")),
//                new IntegerLiteral(1),
//                new IntegerLiteral(1))
//        )
//    );
//
//    // Then:
//    final String doubleCast = CastEvaluator.generateCode(
//        "new BigDecimal(\"1.2\")", SqlTypes.decimal(2, 1), SqlTypes.DOUBLE, ksqlConfig);
//
//    final String longCast = CastEvaluator.generateCode(
//        "1", SqlTypes.INTEGER, SqlTypes.BIGINT, ksqlConfig);
//
//    assertThat(javaExpression, is(
//        "((String) FOO_0.evaluate(" +doubleCast + ", " + longCast + ", " + longCast + "))"
//    ));
//  }
//

//
//  @Test
//  public void shouldEscapeQuotesInStringLiteral() {
//    // Given:
//    final Expression expression = new StringLiteral("\"foo\"");
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(javaExpression, equalTo("\"\\\"foo\\\"\""));
//  }
//
//  @Test
//  public void shouldEscapeQuotesInStringLiteralQuote() {
//    // Given:
//    final Expression expression = new StringLiteral("\\\"");
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(javaExpression, equalTo("\"\\\\\\\"\""));
//  }
//

//
//  @Test
//  public void shouldGenerateCorrectCodeForLikePattern() {
//    // Given:
//    final Expression expression = new LikePredicate(COL1, new StringLiteral("%foo"), Optional.empty());
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(javaExpression, equalTo("LikeEvaluator.matches(COL1, \"%foo\")"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForLikePatternWithEscape() {
//    // Given:
//    final Expression expression = new LikePredicate(COL1, new StringLiteral("%foo"), Optional.of('!'));
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(javaExpression, equalTo("LikeEvaluator.matches(COL1, \"%foo\", '!')"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForLikePatternWithColRef() {
//    // Given:
//    final Expression expression = new LikePredicate(COL1, COL1, Optional.empty());
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(javaExpression, equalTo("LikeEvaluator.matches(COL1, COL1)"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForCaseStatement() {
//    // Given:
//    final Expression expression = new SearchedCaseExpression(
//        ImmutableList.of(
//            new WhenClause(
//                new ComparisonExpression(
//                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(10)),
//                new StringLiteral("small")
//            ),
//            new WhenClause(
//                new ComparisonExpression(
//                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(100)),
//                new StringLiteral("medium")
//            )
//        ),
//        Optional.of(new StringLiteral("large"))
//    );
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // ThenL
//    assertThat(
//        javaExpression, equalTo(
//            "((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.copyOf(Arrays.asList( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(10)) == null) ? false : (COL7 < 10)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(100)) == null) ? false : (COL7 < 100)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }}))), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"large\"; }}))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForCaseStatementWith13Conditions() {
//    // Given:
//    final ImmutableList<Integer> numbers = ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
//    final ImmutableList<String> numberNames = ImmutableList.of("zero", "one", "two", "three", "four", "five",
//        "six", "seven", "eight", "nine", "ten",
//        "eleven", "twelve");
//
//    final ImmutableList<WhenClause> arg = numbers
//        .stream()
//        .map(n -> new WhenClause(
//            new ComparisonExpression(
//                ComparisonExpression.Type.EQUAL, COL7, new IntegerLiteral(n)),
//            new StringLiteral(numberNames.get(n))
//        )).collect(ImmutableList.toImmutableList());
//
//    final Expression expression = new SearchedCaseExpression(
//        arg,
//        Optional.empty()
//    );
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // ThenL
//    assertThat(
//        javaExpression, equalTo(
//            "((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.copyOf(Arrays.asList( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(0)) == null) ? false : ((COL7 <= 0) && (COL7 >= 0))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"zero\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(1)) == null) ? false : ((COL7 <= 1) && (COL7 >= 1))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"one\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(2)) == null) ? false : ((COL7 <= 2) && (COL7 >= 2))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"two\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(3)) == null) ? false : ((COL7 <= 3) && (COL7 >= 3))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"three\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(4)) == null) ? false : ((COL7 <= 4) && (COL7 >= 4))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"four\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(5)) == null) ? false : ((COL7 <= 5) && (COL7 >= 5))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"five\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(6)) == null) ? false : ((COL7 <= 6) && (COL7 >= 6))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"six\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(7)) == null) ? false : ((COL7 <= 7) && (COL7 >= 7))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"seven\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(8)) == null) ? false : ((COL7 <= 8) && (COL7 >= 8))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"eight\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(9)) == null) ? false : ((COL7 <= 9) && (COL7 >= 9))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"nine\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(10)) == null) ? false : ((COL7 <= 10) && (COL7 >= 10))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"ten\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(11)) == null) ? false : ((COL7 <= 11) && (COL7 >= 11))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"eleven\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(12)) == null) ? false : ((COL7 <= 12) && (COL7 >= 12))); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"twelve\"; }}))), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return null; }}))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForCaseStatementWithNoElse() {
//    // Given:
//    final Expression expression = new SearchedCaseExpression(
//        ImmutableList.of(
//            new WhenClause(
//                new ComparisonExpression(
//                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(10)),
//                new StringLiteral("small")
//            ),
//            new WhenClause(
//                new ComparisonExpression(
//                    ComparisonExpression.Type.LESS_THAN, COL7, new IntegerLiteral(100)),
//                new StringLiteral("medium")
//            )
//        ),
//        Optional.empty()
//    );
//
//    // When:
//    final String javaExpression = sqlToJavaVisitor.process(expression);
//
//    // ThenL
//    assertThat(
//        javaExpression, equalTo(
//            "((java.lang.String)SearchedCaseFunction.searchedCaseFunction(ImmutableList.copyOf(Arrays.asList( SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(10)) == null) ? false : (COL7 < 10)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"small\"; }}), SearchedCaseFunction.whenClause( new Supplier<Boolean>() { @Override public Boolean get() { return ((((Object)(COL7)) == null || ((Object)(100)) == null) ? false : (COL7 < 100)); }},  new Supplier<java.lang.String>() { @Override public java.lang.String get() { return \"medium\"; }}))), new Supplier<java.lang.String>() { @Override public java.lang.String get() { return null; }}))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalAdd() {
//    // Given:
//    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
//        Operator.ADD,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(binExp);
//
//    // Then:
//    assertThat(
//        java,
//        is("(COL8.add(COL8, new MathContext(3, RoundingMode.UNNECESSARY)).setScale(1))")
//    );
//  }
//
//  @Test
//  public void shouldGenerateCastLongToDecimalInBinaryExpression() {
//    // Given:
//    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
//        Operator.ADD,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL0"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(binExp);
//
//    // Then:
//    assertThat(java, containsString("DecimalUtil.cast(COL0, 19, 0)"));
//  }
//
//  @Test
//  public void shouldGenerateCastDecimalToDoubleInBinaryExpression() {
//    // Given:
//    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
//        Operator.ADD,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL3"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(binExp);
//
//    // Then:
//    final String doubleCast = CastEvaluator.generateCode(
//        "COL8", SqlTypes.decimal(2, 1), SqlTypes.DOUBLE, ksqlConfig);
//    assertThat(java, containsString(doubleCast));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalSubtract() {
//    // Given:
//    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
//        Operator.SUBTRACT,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(binExp);
//
//    // Then:
//    assertThat(
//        java,
//        is("(COL8.subtract(COL8, new MathContext(3, RoundingMode.UNNECESSARY)).setScale(1))")
//    );
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalMultiply() {
//    // Given:
//    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
//        Operator.MULTIPLY,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(binExp);
//
//    // Then:
//    assertThat(
//        java,
//        is("(COL8.multiply(COL8, new MathContext(5, RoundingMode.UNNECESSARY)).setScale(2))")
//    );
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalDivide() {
//    // Given:
//    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
//        Operator.DIVIDE,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(binExp);
//
//    // Then:
//    assertThat(
//        java,
//        is("(COL8.divide(COL8, new MathContext(8, RoundingMode.UNNECESSARY)).setScale(6))")
//    );
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalMod() {
//    // Given:
//    final ArithmeticBinaryExpression binExp = new ArithmeticBinaryExpression(
//        Operator.MODULUS,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(binExp);
//
//    // Then:
//    assertThat(
//        java,
//        is("(COL8.remainder(COL8, new MathContext(2, RoundingMode.UNNECESSARY)).setScale(1))")
//    );
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalDecimalEQ() {
//    // Given:
//    final ComparisonExpression compExp = new ComparisonExpression(
//        ComparisonExpression.Type.EQUAL,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(compExp);
//
//    // Then:
//    assertThat(java, containsString("(COL8.compareTo(COL9) == 0))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalDecimalGT() {
//    // Given:
//    final ComparisonExpression compExp = new ComparisonExpression(
//        ComparisonExpression.Type.GREATER_THAN,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(compExp);
//
//    // Then:
//    assertThat(java, containsString("(COL8.compareTo(COL9) > 0))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalDecimalGEQ() {
//    // Given:
//    final ComparisonExpression compExp = new ComparisonExpression(
//        ComparisonExpression.Type.GREATER_THAN_OR_EQUAL,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(compExp);
//
//    // Then:
//    assertThat(java, containsString("(COL8.compareTo(COL9) >= 0))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalDecimalLT() {
//    // Given:
//    final ComparisonExpression compExp = new ComparisonExpression(
//        ComparisonExpression.Type.LESS_THAN,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(compExp);
//
//    // Then:
//    assertThat(java, containsString("(COL8.compareTo(COL9) < 0))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalDecimalLEQ() {
//    // Given:
//    final ComparisonExpression compExp = new ComparisonExpression(
//        ComparisonExpression.Type.LESS_THAN_OR_EQUAL,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(compExp);
//
//    // Then:
//    assertThat(java, containsString("(COL8.compareTo(COL9) <= 0))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalDecimalIsDistinct() {
//    // Given:
//    final ComparisonExpression compExp = new ComparisonExpression(
//        ComparisonExpression.Type.IS_DISTINCT_FROM,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL9"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(compExp);
//
//    // Then:
//    assertThat(java, containsString("(COL8.compareTo(COL9) != 0))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalDoubleEQ() {
//    // Given:
//    final ComparisonExpression compExp = new ComparisonExpression(
//        ComparisonExpression.Type.EQUAL,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL3"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(compExp);
//
//    // Then:
//    assertThat(java, containsString("(COL8.compareTo(BigDecimal.valueOf(COL3)) == 0))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDoubleDecimalEQ() {
//    // Given:
//    final ComparisonExpression compExp = new ComparisonExpression(
//        ComparisonExpression.Type.EQUAL,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL3")),
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(compExp);
//
//    // Then:
//    assertThat(java, containsString("(BigDecimal.valueOf(COL3).compareTo(COL8) == 0))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalNegation() {
//    // Given:
//    final ArithmeticUnaryExpression binExp = new ArithmeticUnaryExpression(
//        Optional.empty(),
//        Sign.MINUS,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(binExp);
//
//    // Then:
//    assertThat(java, is("(COL8.negate(new MathContext(2, RoundingMode.UNNECESSARY)))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForDecimalUnaryPlus() {
//    // Given:
//    final ArithmeticUnaryExpression binExp = new ArithmeticUnaryExpression(
//        Optional.empty(),
//        Sign.PLUS,
//        new UnqualifiedColumnReferenceExp(ColumnName.of("COL8"))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(binExp);
//
//    // Then:
//    assertThat(java, is("(COL8.plus(new MathContext(2, RoundingMode.UNNECESSARY)))"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForTimestampTimestampLT() {
//    // Given:
//    final ComparisonExpression compExp = new ComparisonExpression(
//        Type.LESS_THAN,
//        TIMESTAMPCOL,
//        TIMESTAMPCOL
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(compExp);
//
//    // Then:
//    assertThat(java, containsString("(COL10.compareTo(COL10) < 0)"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForTimestampStringEQ() {
//    // Given:
//    final ComparisonExpression compExp = new ComparisonExpression(
//        Type.EQUAL,
//        TIMESTAMPCOL,
//        new StringLiteral("2020-01-01T00:00:00")
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(compExp);
//
//    // Then:
//    assertThat(java, containsString("(COL10.compareTo(SqlTimestamps.parseTimestamp(\"2020-01-01T00:00:00\")) == 0)"));
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForTimestampStringGEQ() {
//    // Given:
//    final ComparisonExpression compExp = new ComparisonExpression(
//        Type.GREATER_THAN_OR_EQUAL,
//        new StringLiteral("2020-01-01T00:00:00"),
//        TIMESTAMPCOL
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(compExp);
//
//    // Then:
//    assertThat(java, containsString("(SqlTimestamps.parseTimestamp(\"2020-01-01T00:00:00\").compareTo(COL10) >= 0)"));
//  }
//
//  @Test
//  public void shouldThrowOnQualifiedColumnReference() {
//    // Given:
//    final Expression expression = new QualifiedColumnReferenceExp(
//        of("foo"),
//        ColumnName.of("bar")
//    );
//
//    // When:
//    assertThrows(
//        UnsupportedOperationException.class,
//        () -> sqlToJavaVisitor.process(expression)
//    );
//  }
//
//  @Test
//  public void shouldGenerateCorrectCodeForInPredicate() {
//    // Given:
//    final Expression expression = new InPredicate(
//        COL0,
//        new InListExpression(ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)))
//    );
//
//    // When:
//    final String java = sqlToJavaVisitor.process(expression);
//
//    // Then:
//    assertThat(java, is("InListEvaluator.matches(COL0,1L,2L)"));
//  }
//
//  @Test
//  public void shouldThrowOnSimpleCase() {
//    // Given:
//    final Expression expression = new SimpleCaseExpression(
//        COL0,
//        ImmutableList.of(new WhenClause(new IntegerLiteral(10), new StringLiteral("ten"))),
//        empty()
//    );
//
//    // When:
//    assertThrows(
//        UnsupportedOperationException.class,
//        () -> sqlToJavaVisitor.process(expression)
//    );
//  }
//
//  @Test
//  public void shouldThrowOnTimeLiteral() {
//    // When:
//    assertThrows(
//        UnsupportedOperationException.class,
//        () -> sqlToJavaVisitor.process(new TimeLiteral("TIME '00:00:00'"))
//    );
//  }
//
  private void givenUdf(
      final String name, final UdfFactory factory, final KsqlScalarFunction function
  ) {
    when(functionRegistry.isAggregate(FunctionName.of(name))).thenReturn(false);
    when(functionRegistry.getUdfFactory(FunctionName.of(name))).thenReturn(factory);
    when(factory.getFunction(anyList())).thenReturn(function);
    when(function.getReturnType(anyList())).thenReturn(SqlTypes.INTEGER);
    final UdfMetadata metadata = mock(UdfMetadata.class);
    when(factory.getMetadata()).thenReturn(metadata);
  }

  private static class AddUdf implements Kudf {

    @Override
    public Object evaluate(Object... args) {
      int a = (Integer) args[0];
      int b = (Integer) args[1];
      return a + b;
    }
  }
}
