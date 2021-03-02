/*
 * Copyright 2021 Confluent Inc.
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

import static io.confluent.ksql.execution.testutil.TestExpressions.ARRAYCOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.MAPCOL;
import static io.confluent.ksql.execution.testutil.TestExpressions.SCHEMA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.codegen.TypeContext;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;
import io.confluent.ksql.execution.expression.tree.LambdaVariable;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.execution.util.FunctionArgumentsUtil.FunctionTypeInfo;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.LambdaType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlLambda;
import io.confluent.ksql.schema.ksql.types.SqlLambdaResolved;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class FunctionArgumentsUtilTest {

  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private UdfFactory udfFactory;
  @Mock
  private KsqlScalarFunction function;

  private ExpressionTypeManager expressionTypeManager;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void init() {
    expressionTypeManager = new ExpressionTypeManager(SCHEMA, functionRegistry);
  }

  @Test
  public void shouldResolveGenericsInLambdaFunction() {
    // Given:
    givenUdfWithNameAndReturnType("ComplexFunction", SqlTypes.DOUBLE);
    when(function.parameters()).thenReturn(
        ImmutableList.of(
            ArrayType.of(GenericType.of("X")),
            MapType.of(GenericType.of("K"), GenericType.of("V")),
            GenericType.of("Q"),
            LambdaType.of(ImmutableList.of(GenericType.of("K"), GenericType.of("V"), GenericType.of("Q")), GenericType.of("K")),
            LambdaType.of(ImmutableList.of(GenericType.of("Q"), GenericType.of("V")), GenericType.of("X"))));

    final FunctionCall expression = givenFunctionCallWithMultipleLambdas();


    // When:
    final FunctionTypeInfo argumentsAndContexts = 
        FunctionArgumentsUtil.getFunctionTypeInfo(expressionTypeManager, expression, udfFactory, new TypeContext());

    // Then:
    assertThat(argumentsAndContexts.getReturnType(), is(SqlTypes.DOUBLE));
    assertThat(argumentsAndContexts.getArgumentInfos().size(), is(5));

    assertThat(argumentsAndContexts.getArgumentInfos().get(3).getSqlArgument(), is(SqlArgument.of(SqlLambdaResolved.of(ImmutableList.of(SqlTypes.BIGINT, SqlTypes.DOUBLE, SqlTypes.STRING), SqlTypes.BIGINT))));
    assertThat(argumentsAndContexts.getArgumentInfos().get(3).getContext().getLambdaType("X"), is(SqlTypes.BIGINT));
    assertThat(argumentsAndContexts.getArgumentInfos().get(3).getContext().getLambdaType("Y"), is(SqlTypes.DOUBLE));
    assertThat(argumentsAndContexts.getArgumentInfos().get(3).getContext().getLambdaType("Z"), is(SqlTypes.STRING));

    assertThat(argumentsAndContexts.getArgumentInfos().get(4).getSqlArgument(), is(SqlArgument.of(SqlLambdaResolved.of(ImmutableList.of(SqlTypes.STRING, SqlTypes.DOUBLE), SqlTypes.DOUBLE))));
    assertThat(argumentsAndContexts.getArgumentInfos().get(4).getContext().getLambdaType("A"), is(SqlTypes.STRING));
    assertThat(argumentsAndContexts.getArgumentInfos().get(4).getContext().getLambdaType("B"), is(SqlTypes.DOUBLE));

    // in the first pass we should have
    verify(udfFactory).getFunction(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.array(SqlTypes.DOUBLE)),
            SqlArgument.of(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.DOUBLE)),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlLambda.of(3)),
            SqlArgument.of(SqlLambda.of(2))
        )
    );
    verify(function).getReturnType(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.array(SqlTypes.DOUBLE)),
            SqlArgument.of(SqlTypes.map(SqlTypes.BIGINT, SqlTypes.DOUBLE)),
            SqlArgument.of(SqlTypes.STRING),
            SqlArgument.of(SqlLambdaResolved.of(ImmutableList.of(SqlTypes.BIGINT, SqlTypes.DOUBLE, SqlTypes.STRING), SqlTypes.BIGINT)),
            SqlArgument.of(SqlLambdaResolved.of(ImmutableList.of(SqlTypes.STRING, SqlTypes.DOUBLE), SqlTypes.DOUBLE))
        )
    );
  }

  @Test
  public void shouldResolveFunctionWithoutLambdas() {
    // Given:
    givenUdfWithNameAndReturnType("NoLambdas", SqlTypes.STRING);
    when(function.parameters()).thenReturn(
        ImmutableList.of(ParamTypes.STRING));

    final FunctionCall expression = new FunctionCall(FunctionName.of("NoLambdas"), ImmutableList.of(new StringLiteral("a")));

    // When:
    final FunctionTypeInfo argumentsAndContexts =
        FunctionArgumentsUtil.getFunctionTypeInfo(expressionTypeManager, expression, udfFactory, new TypeContext());

    // Then:
    assertThat(argumentsAndContexts.getReturnType(), is(SqlTypes.STRING));
    assertThat(argumentsAndContexts.getArgumentInfos().size(), is(1));

    verify(udfFactory).getFunction(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.STRING)
        )
    );

    verify(function).getReturnType(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.STRING)
        )
    );
  }

  @Test
  public void shouldResolveLambdaWithoutGenerics() {
    // Given:
    givenUdfWithNameAndReturnType("SmallLambda", SqlTypes.DOUBLE);
    when(function.parameters()).thenReturn(
        ImmutableList.of(
            ArrayType.of(ParamTypes.DOUBLE), LambdaType.of(ImmutableList.of(ParamTypes.INTEGER), ParamTypes.INTEGER)));

    final FunctionCall expression = new FunctionCall(FunctionName.of("SmallLambda"),
        ImmutableList.of(
            ARRAYCOL,
            new LambdaFunctionCall(ImmutableList.of("2.3"),
                new ArithmeticBinaryExpression(
                    Operator.ADD,
                    new DoubleLiteral(2.3),
                    new DoubleLiteral(2.3)))));

    // When:
    final FunctionTypeInfo argumentsAndContexts =
        FunctionArgumentsUtil.getFunctionTypeInfo(expressionTypeManager, expression, udfFactory, new TypeContext());

    // Then:
    assertThat(argumentsAndContexts.getReturnType(), is(SqlTypes.DOUBLE));
    assertThat(argumentsAndContexts.getArgumentInfos().size(), is(2));

    verify(udfFactory).getFunction(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.array(SqlTypes.DOUBLE)),
            SqlArgument.of(SqlLambda.of(1))
        )
    );

    verify(function).getReturnType(
        ImmutableList.of(
            SqlArgument.of(SqlTypes.array(SqlTypes.DOUBLE)),
            SqlArgument.of(SqlLambdaResolved.of(ImmutableList.of(SqlTypes.INTEGER), SqlTypes.DOUBLE))
        )
    );
  }

  private void givenUdfWithNameAndReturnType(final String name, final SqlType returnType) {
    givenUdfWithNameAndReturnType(name, returnType, udfFactory, function);
  }

  private void givenUdfWithNameAndReturnType(
      final String name, final SqlType returnType, final UdfFactory factory, final KsqlScalarFunction function
  ) {
    when(factory.getFunction(anyList())).thenReturn(function);
    when(function.getReturnType(anyList())).thenReturn(returnType);
  }

  private FunctionCall givenFunctionCallWithMultipleLambdas() {
    return new FunctionCall(
        FunctionName.of("ComplexFunction"),
        ImmutableList.of(
            ARRAYCOL,
            MAPCOL,
            new StringLiteral("q"),
            new LambdaFunctionCall(
                ImmutableList.of("X", "Y", "Z"),
                new SearchedCaseExpression(
                    ImmutableList.of(
                        new WhenClause(
                            new ComparisonExpression(Type.EQUAL, new LambdaVariable("Z"), new StringLiteral("q")),
                            new LambdaVariable("X")
                        )
                    ),
                    Optional.of(new ArithmeticBinaryExpression(
                        Operator.ADD,
                        new LambdaVariable("X"),
                        new IntegerLiteral(5))
                    )
                )
            ),
            new LambdaFunctionCall(
                ImmutableList.of("A", "B"),
                new ArithmeticBinaryExpression(
                    Operator.ADD,
                    new LambdaVariable("B"),
                    new IntegerLiteral(5))
            )));
  }
}
