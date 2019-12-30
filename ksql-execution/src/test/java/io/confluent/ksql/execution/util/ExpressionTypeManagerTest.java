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
import static io.confluent.ksql.execution.testutil.TestExpressions.COL1;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL2;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL3;
import static io.confluent.ksql.execution.testutil.TestExpressions.COL7;
import static io.confluent.ksql.execution.testutil.TestExpressions.MAPCOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.SCHEMA;
import static io.confluent.ksql.execution.testutil.TestExpressions.literal;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.execution.testutil.TestExpressions;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class ExpressionTypeManagerTest {

  private static final SourceName TEST1 = SourceName.of("TEST1");
  private static final ColumnName COL0 = ColumnName.of("COL0");

  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private UdfFactory udfFactory;
  @Mock
  private KsqlScalarFunction function;

  private ExpressionTypeManager expressionTypeManager;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void init() {
    expressionTypeManager = new ExpressionTypeManager(SCHEMA, functionRegistry);

    final UdfFactory internalFactory = mock(UdfFactory.class);
    final UdfMetadata metadata = mock(UdfMetadata.class);
    when(internalFactory.getMetadata()).thenReturn(metadata);

    when(functionRegistry.getUdfFactory(anyString()))
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
    expectedException.expect(KsqlException.class);
    expectedException
        .expectMessage("Operator GREATER_THAN cannot be used to compare BIGINT and STRING");

    // When:
    expressionTypeManager.getExpressionSqlType(expr);

  }

  @Test
  public void shouldFailIfOperatorCannotBeAppiled() {
    // Given:
    final ComparisonExpression expr = new ComparisonExpression(
        Type.GREATER_THAN,
        new BooleanLiteral("true"),
        new BooleanLiteral("false")
    );
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Operator GREATER_THAN cannot be used to compare BOOLEAN");

    // When:
    expressionTypeManager.getExpressionSqlType(expr);

  }

  @Test
  public void shouldFailForComplexTypeComparison() {
    // Given:
    final Expression expression = new ComparisonExpression(Type.GREATER_THAN, MAPCOL, ADDRESS);
    expectedException.expect(KsqlException.class);
    expectedException
        .expectMessage("Operator GREATER_THAN cannot be used to compare MAP and STRUCT");

    // When:
    expressionTypeManager.getExpressionSqlType(expression);
  }

  @Test
  public void shouldFailForCheckingComplexTypeEquality() {
    // Given:
    final Expression expression = new ComparisonExpression(Type.EQUAL, MAPCOL, ADDRESS);
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Operator EQUAL cannot be used to compare MAP and STRUCT");

    // When:
    expressionTypeManager.getExpressionSqlType(expression);

  }

  @Test
  public void shouldEvaluateBooleanSchemaForLikeExpression() {
    final Expression expression = new LikePredicate(COL1, new StringLiteral("%foo"));

    final SqlType exprType0 = expressionTypeManager.getExpressionSqlType(expression);

    assertThat(exprType0, is(SqlTypes.BOOLEAN));
  }

  @Test
  public void shouldEvaluateBooleanSchemaForNotLikeExpression() {
    final Expression expression =
        new NotExpression(new LikePredicate(COL1, new StringLiteral("%foo")));
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
    verify(udfFactory).getFunction(ImmutableList.of(SqlTypes.DOUBLE));
    verify(function).getReturnType(ImmutableList.of(SqlTypes.DOUBLE));
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
    verify(udfFactory).getFunction(ImmutableList.of(SqlTypes.STRING));
    verify(function).getReturnType(ImmutableList.of(SqlTypes.STRING));
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
  public void shouldHandleStructFieldDereference() {
    // Given:
    final Expression expression = new DereferenceExpression(
        Optional.empty(),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL6"))),
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
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL6"))),
        "ZIP"
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not find field 'ZIP' in 'TEST1.COL6'.");

    // When:
    expressionTypeManager.getExpressionSqlType(expression);
  }

  @Test
  public void shouldEvaluateTypeForStructExpression() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(TEST1, COL0, SqlTypes.array(SqlTypes.INTEGER))
        .build();
    expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);

    final Expression exp = new CreateStructExpression(ImmutableList.of(
        new Field("field1", new StringLiteral("foo")),
        new Field("field2", new ColumnReferenceExp(ColumnRef.of(TEST1, COL0))),
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
        .valueColumn(TEST1, COL0, SqlTypes.array(inner))
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
  public void shouldEvaluateTypeForArrayReferenceInStruct() {
    // Given:
    final SqlStruct inner = SqlTypes
        .struct()
        .field("IN0", SqlTypes.array(SqlTypes.INTEGER))
        .build();

    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(TEST1, COL0, inner)
        .build();

    expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);

    final Expression structRef = new DereferenceExpression(
        Optional.empty(),
        new ColumnReferenceExp(ColumnRef.of(TEST1, COL0)),
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
    final SqlType sqlType = SCHEMA.findColumn(ADDRESS.getReference()).get().type();
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
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "WHEN operand type should be boolean."
            + System.lineSeparator()
            + "Type for '(TEST1.COL0 + 10)' is BIGINT"
    );

    // When:
    expressionTypeManager.getExpressionSqlType(expression);
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
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid Case expression. Type for all 'THEN' clauses should be the same."
            + System.lineSeparator()
            + "THEN expression 'WHEN (TEST1.COL0 = 10) THEN 10' has type: INTEGER."
            + System.lineSeparator()
            + "Previous THEN expression(s) type: STRING."
    );

    // When:
    expressionTypeManager.getExpressionSqlType(expression);

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
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid Case expression. Type for the default clause should be the same as for 'THEN' clauses."
            + System.lineSeparator()
            + "THEN type: STRING."
            + System.lineSeparator()
            + "DEFAULT type: BOOLEAN."
    );

    // When:
    expressionTypeManager.getExpressionSqlType(expression);
  }

  @Test
  public void shouldThrowOnTimeLiteral() {
    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    expressionTypeManager.getExpressionSqlType(new TimeLiteral("TIME '00:00:00'"));
  }

  @Test
  public void shouldThrowOnTimestampLiteral() {
    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    expressionTypeManager.getExpressionSqlType(new TimestampLiteral("TIMESTAMP '00:00:00'"));
  }

  @Test
  public void shouldThrowOnIn() {
    // Given:
    final Expression expression = new InPredicate(
        TestExpressions.COL0,
        new InListExpression(ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)))
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    expressionTypeManager.getExpressionSqlType(expression);
  }

  @Test
  public void shouldThrowOnSimpleCase() {
    final Expression expression = new SimpleCaseExpression(
        TestExpressions.COL0,
        ImmutableList.of(new WhenClause(new IntegerLiteral(10), new StringLiteral("ten"))),
        Optional.empty()
    );

    // Then:
    expectedException.expect(UnsupportedOperationException.class);

    // When:
    expressionTypeManager.getExpressionSqlType(expression);
  }

  private void givenUdfWithNameAndReturnType(final String name, final SqlType returnType) {
    givenUdfWithNameAndReturnType(name, returnType, udfFactory, function);
  }

  private void givenUdfWithNameAndReturnType(
      final String name, final SqlType returnType, final UdfFactory factory, final KsqlScalarFunction function
  ) {
    when(functionRegistry.isAggregate(name)).thenReturn(false);
    when(functionRegistry.getUdfFactory(name)).thenReturn(factory);
    when(factory.getFunction(anyList())).thenReturn(function);
    when(function.getReturnType(anyList())).thenReturn(returnType);
    final UdfMetadata metadata = mock(UdfMetadata.class);
    when(factory.getMetadata()).thenReturn(metadata);
  }
}
