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

package io.confluent.ksql.execution.util;

import static io.confluent.ksql.execution.testutil.TestExpressions.ADDRESS;
import static io.confluent.ksql.execution.testutil.TestExpressions.ARRAYCOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL1;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL2;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL3;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL7;
import static io.confluent.ksql.execution.testutil.TestExpressions.MAPCOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.SCHEMA;
import static io.confluent.ksql.execution.testutil.TestExpressions.literal;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.BytesLiteral;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.DateLiteral;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;
import io.confluent.ksql.execution.expression.tree.LambdaVariable;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.execution.testutil.TestExpressions;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.DoubleType;
import io.confluent.ksql.function.types.IntegerType;
import io.confluent.ksql.function.types.LambdaType;
import io.confluent.ksql.function.types.LongType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.StringType;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlLambda;
import io.confluent.ksql.schema.ksql.types.SqlLambdaResolved;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class ExpressionTypeManagerTest {

  private static final ColumnName COL0 = ColumnName.of("COL0");

  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private UdfFactory udfFactory;
  @Mock
  private KsqlScalarFunction function;

  @Mock
  private AggregateFunctionFactory aggregateFactory;
  @Mock
  private KsqlAggregateFunction aggregateFunction;
  @Mock
  private Function<String, KsqlAggregateFunction<?, ?, ?>> aggCreator;

  private ExpressionTypeManager expressionTypeManager;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void init() {
    expressionTypeManager = new ExpressionTypeManager(SCHEMA, functionRegistry);

    final UdfFactory internalFactory = mock(UdfFactory.class);
    final UdfMetadata metadata = mock(UdfMetadata.class);
    when(internalFactory.getMetadata()).thenReturn(metadata);

    when(functionRegistry.getUdfFactory(any()))
        .thenReturn(internalFactory);
  }

  @Test
  public void shouldResolveTypeForAddBigIntDouble() {
    final Expression expression = new ArithmeticBinaryExpression(Operator.ADD, TestExpressions.COL0,
        COL3
    );

    final SqlType type = expressionTypeManager.getExpressionSqlType(expression);

    assertThat(type, is(SqlTypes.DOUBLE));
  }

  @Test
  public void shouldResolveTypeForAddDoubleIntegerLiteral() {
    final Expression expression = new ArithmeticBinaryExpression(Operator.ADD, COL3, literal(10));

    final SqlType type = expressionTypeManager.getExpressionSqlType(expression);

    assertThat(type, is(SqlTypes.DOUBLE));
  }

  @Test
  public void shouldResolveTypeForAddBigintIntegerLiteral() {
    final Expression expression = new ArithmeticBinaryExpression(Operator.ADD, TestExpressions.COL0,
        literal(10)
    );

    final SqlType type = expressionTypeManager.getExpressionSqlType(expression);

    assertThat(type, is(SqlTypes.BIGINT));
  }

  @Test
  public void shouldResolveTypeForMultiplyBigintIntegerLiteral() {
    final Expression expression =
        new ArithmeticBinaryExpression(Operator.MULTIPLY, TestExpressions.COL0, literal(10));

    final SqlType type = expressionTypeManager.getExpressionSqlType(expression);

    assertThat(type, is(SqlTypes.BIGINT));
  }

  @Test
  public void testComparisonExpr() {
    final Expression expression = new ComparisonExpression(Type.GREATER_THAN, TestExpressions.COL0,
        COL3
    );

    final SqlType exprType = expressionTypeManager.getExpressionSqlType(expression);

    assertThat(exprType, is(SqlTypes.BOOLEAN));
  }

  @Test
  public void shouldFailIfComparisonOperandsAreIncompatible() {
    // Given:
    final ComparisonExpression expr = new ComparisonExpression(Type.GREATER_THAN,
        TestExpressions.COL0, COL1
    );

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> expressionTypeManager.getExpressionSqlType(expr)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot compare BIGINT to STRING with GREATER_THAN"
    ));
    assertThat(e.getUnloggedMessage(), containsString(
        "Cannot compare COL0 (BIGINT) to COL1 (STRING) with GREATER_THAN"
    ));
  }

  @Test
  public void shouldFailIfOperatorCannotBeAppiled() {
    // Given:
    final ComparisonExpression expr = new ComparisonExpression(
        Type.GREATER_THAN,
        new BooleanLiteral("true"),
        new BooleanLiteral("false")
    );

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> expressionTypeManager.getExpressionSqlType(expr)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot compare BOOLEAN to BOOLEAN with "
            + "GREATER_THAN"
    ));
    assertThat(e.getUnloggedMessage(), containsString(
        "Cannot compare true (BOOLEAN) to false (BOOLEAN) with "
            + "GREATER_THAN"
    ));
  }

  @Test
  public void shouldFailForComplexTypeComparison() {
    // Given:
    final Expression expression = new ComparisonExpression(Type.GREATER_THAN, MAPCOL, ADDRESS);

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot compare MAP<BIGINT, DOUBLE>"
            + " to STRUCT<`NUMBER` BIGINT, `STREET` STRING, `CITY` STRING,"
            + " `STATE` STRING, `ZIPCODE` BIGINT> with GREATER_THAN."
    ));
    assertThat(e.getUnloggedMessage(), containsString(
        "Cannot compare COL5 (MAP<BIGINT, DOUBLE>) to COL6 (STRUCT<`NUMBER` BIGINT, "
            + "`STREET` STRING, `CITY` STRING, `STATE` STRING, `ZIPCODE` BIGINT>) "
            + "with GREATER_THAN"
    ));
  }

  @Test
  public void shouldFailForCheckingComplexTypeEquality() {
    // Given:
    final Expression expression = new ComparisonExpression(Type.EQUAL, MAPCOL, ADDRESS);

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot compare MAP<BIGINT, DOUBLE> "
            + "to STRUCT<`NUMBER` BIGINT, `STREET` STRING, `CITY` STRING,"
            + " `STATE` STRING, `ZIPCODE` BIGINT> with EQUAL."
    ));
    assertThat(e.getUnloggedMessage(), containsString(
        "Cannot compare COL5 (MAP<BIGINT, DOUBLE>) to COL6 "
            + "(STRUCT<`NUMBER` BIGINT, `STREET` STRING, `CITY` STRING, `STATE` STRING, "
            + "`ZIPCODE` BIGINT>) with EQUAL"
    ));
  }

  @Test
  public void shouldFailOnQualfiedColumnReference() {
    // Given:
    final Expression expression = new QualifiedColumnReferenceExp(
        SourceName.of("foo"),
        ColumnName.of("bar")
    );

    // When:
    assertThrows(
        IllegalStateException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );
  }

  @Test
  public void shouldEvaluateBooleanSchemaForLikeExpression() {
    final Expression expression = new LikePredicate(COL1, new StringLiteral("%foo"), Optional.empty());

    final SqlType exprType0 = expressionTypeManager.getExpressionSqlType(expression);

    assertThat(exprType0, is(SqlTypes.BOOLEAN));
  }

  @Test
  public void shouldEvaluateBooleanSchemaForNotLikeExpression() {
    final Expression expression =
        new NotExpression(new LikePredicate(COL1, new StringLiteral("%foo"), Optional.empty()));
    final SqlType exprType0 = expressionTypeManager.getExpressionSqlType(expression);
    assertThat(exprType0, is(SqlTypes.BOOLEAN));
  }

  @Test
  public void shouldEvaluateBooleanSchemaForInExpression() {
    final Expression expression = new InPredicate(
        TestExpressions.COL0,
        new InListExpression(ImmutableList.of(new StringLiteral("key1"))));

    final SqlType exprType0 = expressionTypeManager.getExpressionSqlType(expression);

    assertThat(exprType0, is(SqlTypes.BOOLEAN));
  }


  @Test
  public void shouldEvaluateTypeForUDF() {
    // Given:
    givenUdfWithNameAndReturnType("FLOOR", SqlTypes.DOUBLE);
    final Expression expression =
        new FunctionCall(FunctionName.of("FLOOR"), ImmutableList.of(COL3));

    // When:
    final SqlType exprType = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(exprType, is(SqlTypes.DOUBLE));
    verify(udfFactory).getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.DOUBLE)));
    verify(function).getReturnType(ImmutableList.of(SqlArgument.of(SqlTypes.DOUBLE)));
  }

  @Test
  public void shouldEvaluateTypeForStringUDF() {
    // Given:
    givenUdfWithNameAndReturnType("LCASE", SqlTypes.STRING);
    final Expression expression =
        new FunctionCall(FunctionName.of("LCASE"), ImmutableList.of(COL2));

    // When:
    final SqlType exprType = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(exprType, is(SqlTypes.STRING));
    verify(udfFactory).getFunction(ImmutableList.of(SqlArgument.of(SqlTypes.STRING)));
    verify(function).getReturnType(ImmutableList.of(SqlArgument.of(SqlTypes.STRING)));
  }

  @Test
  public void shouldHandleNestedUdfs() {
    // Given:
    givenUdfWithNameAndReturnType("EXTRACTJSONFIELD", SqlTypes.STRING);
    final UdfFactory outerFactory = mock(UdfFactory.class);
    final KsqlScalarFunction function = mock(KsqlScalarFunction.class);
    givenUdfWithNameAndReturnType("LCASE", SqlTypes.STRING, outerFactory, function);
    final Expression inner = new FunctionCall(
        FunctionName.of("EXTRACTJSONFIELD"),
        ImmutableList.of(COL1, new StringLiteral("$.name)"))
    );
    final Expression expression =
        new FunctionCall(FunctionName.of("LCASE"), ImmutableList.of(inner));

    // When/Then:
    assertThat(expressionTypeManager.getExpressionSqlType(expression), equalTo(SqlTypes.STRING));
  }

  @Test
  public void shouldEvaluateLambdaInUDFWithArray() {
    // Given:
    givenUdfWithNameAndReturnType("TRANSFORM", SqlTypes.DOUBLE);
    when(function.parameters()).thenReturn(
        ImmutableList.of(
            ArrayType.of(DoubleType.INSTANCE),
            LambdaType.of(ImmutableList.of(DoubleType.INSTANCE), DoubleType.INSTANCE)));

    final Expression expression =
        new FunctionCall(
            FunctionName.of("TRANSFORM"),
            ImmutableList.of(
                ARRAYCOL,
                new LambdaFunctionCall(
                    ImmutableList.of("X"),
                    new ArithmeticBinaryExpression(
                        Operator.ADD,
                        new LambdaVariable("X"),
                        new IntegerLiteral(5))
                )));

    // When:
    final SqlType exprType = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(exprType, is(SqlTypes.DOUBLE));
    verify(udfFactory).getFunction(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.array(SqlTypes.DOUBLE)),
            SqlArgument.of(SqlLambda.of(1))));
    verify(function).getReturnType(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.array(SqlTypes.DOUBLE)),
            SqlArgument.of(SqlLambdaResolved.of(ImmutableList.of(SqlTypes.DOUBLE), SqlTypes.DOUBLE))));
  }

  @Test
  public void shouldEvaluateLambdaInUDFWithMap() {
    // Given:
    givenUdfWithNameAndReturnType("TRANSFORM", SqlTypes.DOUBLE);
    when(function.parameters()).thenReturn(
        ImmutableList.of(
            MapType.of(DoubleType.INSTANCE, DoubleType.INSTANCE),
            LambdaType.of(ImmutableList.of(LongType.INSTANCE, DoubleType.INSTANCE), DoubleType.INSTANCE)));

    final Expression expression =
        new FunctionCall(
            FunctionName.of("TRANSFORM"),
            ImmutableList.of(
                MAPCOL,
                new LambdaFunctionCall(
                    ImmutableList.of("X", "Y"),
                    new ArithmeticBinaryExpression(
                        Operator.ADD,
                        new LambdaVariable("X"),
                        new IntegerLiteral(5))
                )));

    // When:
    final SqlType exprType = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(exprType, is(SqlTypes.DOUBLE));
    verify(udfFactory).getFunction(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.DOUBLE)),
            SqlArgument.of(SqlLambda.of(2))));
    verify(function).getReturnType(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.DOUBLE)),
            SqlArgument.of(
                SqlLambdaResolved.of(ImmutableList.of(SqlTypes.BIGINT, SqlTypes.DOUBLE), SqlTypes.BIGINT))));
  }

  @Test
  public void shouldEvaluateAnyNumberOfArgumentLambda() {
    // Given:
    givenUdfWithNameAndReturnType("TRANSFORM", SqlTypes.STRING);
    when(function.parameters()).thenReturn(
        ImmutableList.of(
            ArrayType.of(DoubleType.INSTANCE),
            StringType.INSTANCE,
            MapType.of(LongType.INSTANCE, DoubleType.INSTANCE),
            LambdaType.of(
                ImmutableList.of(
                    DoubleType.INSTANCE,
                    StringType.INSTANCE,
                    LongType.INSTANCE,
                    DoubleType.INSTANCE
                ),
                StringType.INSTANCE)));

    final Expression expression =
        new FunctionCall(
            FunctionName.of("TRANSFORM"),
            ImmutableList.of(
                ARRAYCOL,
                new StringLiteral("Q"),
                MAPCOL,
                new LambdaFunctionCall(
                    ImmutableList.of("A", "B", "C", "D"),
                    new ArithmeticBinaryExpression(
                        Operator.ADD,
                        new LambdaVariable("C"),
                        new IntegerLiteral(5))
                )));

    // When:
    final SqlType exprType = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(exprType, is(SqlTypes.STRING));
    verify(udfFactory).getFunction(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.array(SqlTypes.DOUBLE)),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.DOUBLE)),
            SqlArgument.of(SqlLambda.of(4))));
    verify(function).getReturnType(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.array(SqlTypes.DOUBLE)),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.DOUBLE)),
            SqlArgument.of(SqlLambdaResolved
                .of(ImmutableList.of(SqlTypes.DOUBLE, SqlTypes.STRING, SqlTypes.BIGINT, SqlTypes.DOUBLE), SqlTypes.BIGINT))));
  }

  @Test
  public void shouldEvaluateLambdaArgsToType() {
    // Given:
    givenUdfWithNameAndReturnType("TRANSFORM", SqlTypes.STRING);
    when(function.parameters()).thenReturn(
        ImmutableList.of(
            ArrayType.of(DoubleType.INSTANCE),
            StringType.INSTANCE,
            LambdaType.of(
                ImmutableList.of(
                    DoubleType.INSTANCE,
                    StringType.INSTANCE
                ),
                StringType.INSTANCE)));
    final Expression expression =
        new FunctionCall(
            FunctionName.of("TRANSFORM"),
            ImmutableList.of(
                ARRAYCOL,
                new StringLiteral("Q"),
                new LambdaFunctionCall(
                    ImmutableList.of("A", "B"),
                    new ArithmeticBinaryExpression(
                        Operator.ADD,
                        new LambdaVariable("A"),
                        new LambdaVariable("B"))
                )));

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getUnloggedMessage(), Matchers.containsString(
        "Error processing expression: (A + B). " +
            "Unsupported arithmetic types. DOUBLE STRING\n" +
            "Statement: (A + B)"));
    assertThat(e.getMessage(), Matchers.is(
        "Error processing expression."));
  }

  @Test
  public void shouldFailToEvaluateLambdaWithMismatchedArgumentNumber() {
    // Given:
    givenUdfWithNameAndReturnType("TRANSFORM", SqlTypes.DOUBLE);
    when(function.parameters()).thenReturn(
        ImmutableList.of(
            ArrayType.of(DoubleType.INSTANCE),
            LambdaType.of(
                ImmutableList.of(
                    DoubleType.INSTANCE
                ),
                StringType.INSTANCE
            )
        )
    );
    final Expression expression =
        new FunctionCall(
            FunctionName.of("TRANSFORM"),
            ImmutableList.of(
                ARRAYCOL,
                new LambdaFunctionCall(
                    ImmutableList.of("X", "Y"),
                    new ArithmeticBinaryExpression(
                        Operator.ADD,
                        new LambdaVariable("X"),
                        new IntegerLiteral(5))
                )));

    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getMessage(), Matchers.containsString(
        "Was expecting 1 arguments but found 2, [X, Y]. Check your lambda statement."));
  }

  @Test
  public void shouldHandleMultipleLambdasInSameFunctionCallWithDifferentVariableNames() {
    // Given:
    givenUdfWithNameAndReturnType("TRANSFORM", SqlTypes.INTEGER);
    when(function.parameters()).thenReturn(
        ImmutableList.of(
            MapType.of(LongType.INSTANCE, DoubleType.INSTANCE),
            IntegerType.INSTANCE,
            LambdaType.of(
                ImmutableList.of(
                    DoubleType.INSTANCE,
                    DoubleType.INSTANCE
                ),
                StringType.INSTANCE),
            LambdaType.of(
                ImmutableList.of(
                    DoubleType.INSTANCE,
                    DoubleType.INSTANCE
                ),
                StringType.INSTANCE
            )
        ));
    final Expression expression = new ArithmeticBinaryExpression(
        Operator.ADD,
        new FunctionCall(
            FunctionName.of("TRANSFORM"),
            ImmutableList.of(
                MAPCOL,
                new IntegerLiteral(0),
                new LambdaFunctionCall(
                    ImmutableList.of("A", "B"),
                    new ArithmeticBinaryExpression(
                        Operator.ADD,
                        new LambdaVariable("A"),
                        new LambdaVariable("B"))
                ),
                new LambdaFunctionCall(
                    ImmutableList.of("K", "V"),
                    new ArithmeticBinaryExpression(
                        Operator.ADD,
                        new LambdaVariable("K"),
                        new LambdaVariable("V"))
                ))),
        new IntegerLiteral(5)
    );

    // When:
    final SqlType result = expressionTypeManager.getExpressionSqlType(expression);

    assertThat(result, is(SqlTypes.INTEGER));
  }

  @Test
  public void shouldHandleNestedLambdas() {
    // Given:
    givenUdfWithNameAndReturnType("TRANSFORM", SqlTypes.INTEGER);
    when(function.parameters()).thenReturn(
        ImmutableList.of(
            ArrayType.of(LongType.INSTANCE),
            IntegerType.INSTANCE,
            LambdaType.of(
                ImmutableList.of(
                    DoubleType.INSTANCE,
                    DoubleType.INSTANCE
                ),
                StringType.INSTANCE),
            LambdaType.of(
                ImmutableList.of(
                    DoubleType.INSTANCE,
                    DoubleType.INSTANCE
                ),
                StringType.INSTANCE
            )
        ));
    final Expression expression = new ArithmeticBinaryExpression(
        Operator.ADD,
        new FunctionCall(
            FunctionName.of("TRANSFORM"),
            ImmutableList.of(
                ARRAYCOL,
                new IntegerLiteral(0),
                new LambdaFunctionCall(
                    ImmutableList.of("A", "B"),
                    new ArithmeticBinaryExpression(
                        Operator.ADD,
                        new FunctionCall(
                            FunctionName.of("TRANSFORM"),
                            ImmutableList.of(
                                ARRAYCOL,
                                new IntegerLiteral(0),
                                new LambdaFunctionCall(
                                    ImmutableList.of("Q", "V"),
                                    new ArithmeticBinaryExpression(
                                        Operator.ADD,
                                        new LambdaVariable("Q"),
                                        new LambdaVariable("V"))
                                ))),
                        new LambdaVariable("B"))
                ))),
        new IntegerLiteral(5)
    );

    // When:
    final SqlType result = expressionTypeManager.getExpressionSqlType(expression);

    assertThat(result, is(SqlTypes.INTEGER));
  }

  @Test
  public void shouldHandleStructFieldDereference() {
    // Given:
    final Expression expression = new DereferenceExpression(
        Optional.empty(),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL6")),
        "STREET"
    );

    // When:
    final SqlType result = expressionTypeManager.getExpressionSqlType(expression);

    assertThat(result, is(SqlTypes.STRING));
  }

  @Test
  public void shouldFailIfThereIsInvalidFieldNameInStructCall() {
    // Given:
    final Expression expression = new DereferenceExpression(
        Optional.empty(),
        new UnqualifiedColumnReferenceExp(ColumnName.of("COL6")),
        "ZIP"
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Could not find field 'ZIP' in 'COL6'."));
  }

  @Test
  public void shouldEvaluateTypeForCreateArrayExpression() {
    // Given:
    Expression expression = new CreateArrayExpression(
        ImmutableList.of(new UnqualifiedColumnReferenceExp(COL0))
    );

    // When:
    final SqlType type = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(type, is(SqlTypes.array(SqlTypes.BIGINT)));
  }


  @Test
  public void shouldEvaluateTypeForCreateArrayExpressionWithNull() {
    // Given:
    Expression expression = new CreateArrayExpression(
        ImmutableList.of(
            new UnqualifiedColumnReferenceExp(COL0),
            new NullLiteral()
        )
    );

    // When:
    final SqlType type = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(type, is(SqlTypes.array(SqlTypes.BIGINT)));
  }

  @Test
  public void shouldThrowOnArrayAllNulls() {
    // Given:
    Expression expression = new CreateArrayExpression(
        ImmutableList.of(
            new NullLiteral()
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot construct an array with all NULL elements"));
  }

  @Test
  public void shouldThrowOnArrayMultipleTypes() {
    // Given:
    Expression expression = new CreateArrayExpression(
        ImmutableList.of(
            new UnqualifiedColumnReferenceExp(COL0),
            new StringLiteral("foo")
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "invalid input syntax for type BIGINT: \"foo\"."));
  }

  @Test
  public void shouldEvaluateTypeForCreateMapExpression() {
    // Given:
    Expression expression = new CreateMapExpression(
        ImmutableMap.of(
            COL3, new UnqualifiedColumnReferenceExp(COL0)
        )
    );

    // When:
    final SqlType type = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(type, is(SqlTypes.map(SqlTypes.DOUBLE, SqlTypes.BIGINT)));
  }

  @Test
  public void shouldThrowOnMapOfMultipleTypes() {
    // Given:
    Expression expression = new CreateMapExpression(
        ImmutableMap.of(
            new StringLiteral("foo"),
            new UnqualifiedColumnReferenceExp(COL0),
            new StringLiteral("bar"),
            new StringLiteral("bar")
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "invalid input syntax for type BIGINT: \"bar\""));
  }

  @Test
  public void shouldThrowOnMapOfNullValues() {
    // Given:
    Expression expression = new CreateMapExpression(
        ImmutableMap.of(
            new StringLiteral("foo"),
            new NullLiteral()
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot construct a map with all NULL values"));
  }

  @Test
  public void shouldEvaluateTypeForStructExpression() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.array(SqlTypes.INTEGER))
        .build();

    expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);

    final Expression exp = new CreateStructExpression(ImmutableList.of(
        new Field("field1", new StringLiteral("foo")),
        new Field("field2", new UnqualifiedColumnReferenceExp(COL0)),
        new Field("field3", new CreateStructExpression(ImmutableList.of()))
    ));

    // When:
    final SqlType sqlType = expressionTypeManager.getExpressionSqlType(exp);

    // Then:
    assertThat(sqlType,
        is(SqlTypes.struct()
            .field("field1", SqlTypes.STRING)
            .field("field2", SqlTypes.array(SqlTypes.INTEGER))
            .field("field3", SqlTypes.struct().build())
            .build()));
  }

  @Test
  public void shouldEvaluateTypeForStructDereferenceInArray() {
    // Given:
    final SqlStruct inner = SqlTypes.struct().field("IN0", SqlTypes.INTEGER).build();

    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.array(inner))
        .build();

    expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);

    final Expression expression = new DereferenceExpression(
        Optional.empty(),
        new SubscriptExpression(TestExpressions.COL0, new IntegerLiteral(1)),
        "IN0"
    );

    // When:
    final SqlType result = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(result, is(SqlTypes.INTEGER));
  }

  @Test
  public void shouldThrowGoodErrorMessageForSubscriptOnStruct() {
    // Given:
    final SqlStruct structType = SqlTypes.struct().field("IN0", SqlTypes.INTEGER).build();

    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(COL0, structType)
        .build();

    expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);

    final Expression expression = new SubscriptExpression(
        Optional.empty(),
        TestExpressions.COL0,
        new StringLiteral("IN0")
    );

    // When:
    final UnsupportedOperationException e = assertThrows(
        UnsupportedOperationException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression));

    // Then:
    assertThat(
        e.getMessage(),
        is("Subscript expression (COL0['IN0']) do not apply to STRUCT<`IN0` INTEGER>. "
            + "Use the dereference operator for STRUCTS: COL0->'IN0'"));
  }

  @Test
  public void shouldThrowGoodErrorMessageForSubscriptOnScalar() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.INTEGER)
        .build();

    expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);

    final Expression expression = new SubscriptExpression(
        Optional.empty(),
        TestExpressions.COL0,
        new StringLiteral("IN0")
    );

    // When:
    final UnsupportedOperationException e = assertThrows(
        UnsupportedOperationException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression));

    // Then:
    assertThat(
        e.getMessage(),
        is("Subscript expression (COL0['IN0']) do not apply to INTEGER."));
  }

  @Test
  public void shouldEvaluateTypeForArrayReferenceInStruct() {
    // Given:
    final SqlStruct inner = SqlTypes
        .struct()
        .field("IN0", SqlTypes.array(SqlTypes.INTEGER))
        .build();

    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(COL0, inner)
        .build();

    expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);

    final Expression structRef = new DereferenceExpression(
        Optional.empty(),
        new UnqualifiedColumnReferenceExp(COL0),
        "IN0"
    );

    final Expression expression = new SubscriptExpression(structRef, new IntegerLiteral(1));

    // When:
    final SqlType result = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(result, is(SqlTypes.INTEGER));
  }

  @Test
  public void shouldGetCorrectSchemaForSearchedCase() {
    // Given:
    final Expression expression = new SearchedCaseExpression(
        ImmutableList.of(
            new WhenClause(
                new ComparisonExpression(Type.LESS_THAN, COL7, new IntegerLiteral(10)),
                new StringLiteral("small")
            ),
            new WhenClause(
                new ComparisonExpression(Type.LESS_THAN, COL7, new IntegerLiteral(100)),
                new StringLiteral("medium")
            )
        ),
        Optional.of(new StringLiteral("large"))
    );

    // When:
    final SqlType result =
        expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(result, is(SqlTypes.STRING));

  }

  @Test
  public void shouldGetCorrectSchemaForSearchedCaseWhenStruct() {
    // Given:
    final Expression expression = new SearchedCaseExpression(
        ImmutableList.of(
            new WhenClause(
                new ComparisonExpression(Type.EQUAL, TestExpressions.COL0, new IntegerLiteral(10)),
                ADDRESS
            )
        ),
        Optional.empty()
    );

    // When:
    final SqlType result = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    final SqlType sqlType = SCHEMA.findColumn(ADDRESS.getColumnName()).get().type();
    assertThat(result, is(sqlType));
  }

  @Test
  public void shouldFailIfWhenIsNotBoolean() {
    // Given:
    final Expression expression = new SearchedCaseExpression(
        ImmutableList.of(
            new WhenClause(
                new ArithmeticBinaryExpression(Operator.ADD, TestExpressions.COL0,
                    new IntegerLiteral(10)
                ),
                new StringLiteral("foo")
            )
        ),
        Optional.empty()
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "WHEN operand type should be boolean."
            + System.lineSeparator()
            + "Type for '(COL0 + 10)' is BIGINT"
    ));
  }

  @Test
  public void shouldFailOnInconsistentWhenResultType() {
    // Given:
    final Expression expression = new SearchedCaseExpression(
        ImmutableList.of(
            new WhenClause(
                new ComparisonExpression(Type.EQUAL, TestExpressions.COL0, new IntegerLiteral(100)),
                new StringLiteral("one-hundred")
            ),
            new WhenClause(
                new ComparisonExpression(Type.EQUAL, TestExpressions.COL0, new IntegerLiteral(10)),
                new IntegerLiteral(10)
            )
        ),
        Optional.empty()
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Invalid Case expression. Type for all 'THEN' clauses should be the same."
            + System.lineSeparator()
            + "THEN expression 'WHEN (COL0 = 10) THEN 10' has type: INTEGER."
            + System.lineSeparator()
            + "Previous THEN expression(s) type: STRING."
    ));

  }

  @Test
  public void shouldFailIfDefaultHasDifferentTypeToWhen() {
    // Given:
    final Expression expression = new SearchedCaseExpression(
        ImmutableList.of(
            new WhenClause(
                new ComparisonExpression(Type.EQUAL, TestExpressions.COL0, new IntegerLiteral(10)),
                new StringLiteral("good")
            )
        ),
        Optional.of(new BooleanLiteral("true"))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Invalid Case expression. Type for the default clause should be the same as for 'THEN' clauses."
            + System.lineSeparator()
            + "THEN type: STRING."
            + System.lineSeparator()
            + "DEFAULT type: BOOLEAN."
    ));
  }

  @Test
  public void shouldProcessTimeLiteral() {
    assertThat(expressionTypeManager.getExpressionSqlType(new TimeLiteral(new Time(1000))), is(SqlTypes.TIME));
  }

  @Test
  public void shouldProcessDateLiteral() {
    assertThat(expressionTypeManager.getExpressionSqlType(new DateLiteral(new Date(86400000))), is(SqlTypes.DATE));
  }

  @Test
  public void shouldProcessBytesLiteral() {
    assertThat(expressionTypeManager.getExpressionSqlType(new BytesLiteral(ByteBuffer.wrap(new byte[] {123}))), is(SqlTypes.BYTES));
  }

  @Test
  public void shouldReturnBooleanForInPredicate() {
    // Given:
    final Expression expression = new InPredicate(
        TestExpressions.COL0,
        new InListExpression(ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)))
    );

    // When:
    final SqlType result = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(result, is(SqlTypes.BOOLEAN));
  }

  @Test
  public void shouldThrowOnSimpleCase() {
    final Expression expression = new SimpleCaseExpression(
        TestExpressions.COL0,
        ImmutableList.of(new WhenClause(new IntegerLiteral(10), new StringLiteral("ten"))),
        Optional.empty()
    );

    // When:
    assertThrows(
        UnsupportedOperationException.class,
        () -> expressionTypeManager.getExpressionSqlType(expression)
    );
  }

  @Test
  public void shouldEvaluateTypeForMultiParamUdaf() {
    // Given:
    givenUdafWithNameAndReturnType("TEST_ETM", SqlTypes.STRING, aggregateFactory, aggregateFunction);
    final Expression expression = new FunctionCall(
            FunctionName.of("TEST_ETM"),
            ImmutableList.of(
                    COL7,
                    new UnqualifiedColumnReferenceExp(ColumnName.of("COL4")),
                    new StringLiteral("hello world")
            )
    );

    // When:
    final SqlType exprType = expressionTypeManager.getExpressionSqlType(expression);

    // Then:
    assertThat(exprType, is(SqlTypes.STRING));
    verify(aggregateFactory).getFunction(ImmutableList.of(SqlTypes.INTEGER, SqlArray.of(SqlTypes.DOUBLE), SqlTypes.STRING));
    verify(aggCreator).apply("hello world");
    verify(aggregateFunction).returnType();
  }

  private void givenUdfWithNameAndReturnType(final String name, final SqlType returnType) {
    givenUdfWithNameAndReturnType(name, returnType, udfFactory, function);
  }

  private void givenUdfWithNameAndReturnType(
      final String name, final SqlType returnType, final UdfFactory factory, final KsqlScalarFunction function
  ) {
    when(functionRegistry.isAggregate(FunctionName.of(name))).thenReturn(false);
    when(functionRegistry.getUdfFactory(FunctionName.of(name))).thenReturn(factory);
    when(factory.getFunction(anyList())).thenReturn(function);
    when(function.getReturnType(anyList())).thenReturn(returnType);
    final UdfMetadata metadata = mock(UdfMetadata.class);
    when(factory.getMetadata()).thenReturn(metadata);
  }

  private void givenUdafWithNameAndReturnType(
          final String name, final SqlType returnType, final AggregateFunctionFactory factory,
          final KsqlAggregateFunction function
  ) {
    when(functionRegistry.isAggregate(FunctionName.of(name))).thenReturn(true);
    when(functionRegistry.getAggregateFactory(FunctionName.of(name))).thenReturn(factory);
    when(factory.getFunction(eq(Arrays.asList(SqlTypes.INTEGER, SqlArray.of(SqlTypes.DOUBLE), SqlTypes.STRING))))
            .thenReturn(new AggregateFunctionFactory.FunctionSource(1, (initArgs) -> aggCreator.apply((String) initArgs.arg(0))));
    when(aggCreator.apply(any())).thenReturn(function);
    when(function.returnType()).thenReturn(returnType);
    final UdfMetadata metadata = mock(UdfMetadata.class);
    when(factory.getMetadata()).thenReturn(metadata);
  }
}
