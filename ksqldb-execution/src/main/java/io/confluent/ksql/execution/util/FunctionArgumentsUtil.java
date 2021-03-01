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

import io.confluent.ksql.execution.codegen.TypeContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;
import io.confluent.ksql.function.GenericsUtil;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.LambdaType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlLambda;
import io.confluent.ksql.schema.ksql.types.SqlLambdaResolved;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class FunctionArgumentsUtil {

  private FunctionArgumentsUtil() {

  }

  public static class FunctionArgumentsAndContext {
    private final List<TypeContext> contexts;
    private final List<SqlArgument> arguments;
    private final SqlType returnType;
    private final KsqlScalarFunction function;

    public FunctionArgumentsAndContext(
        final List<TypeContext> contexts,
        final List<SqlArgument> arguments,
        final SqlType returnType,
        final KsqlScalarFunction function
    ) {
      this.contexts = contexts;
      this.arguments = arguments;
      this.returnType = returnType;
      this.function = function;
    }

    public List<TypeContext> getContexts() {
      return contexts;
    }

    public List<SqlArgument> getArguments() {
      return arguments;
    }

    public SqlType getReturnType() {
      return returnType;
    }

    public KsqlScalarFunction getFunction() {
      return function;
    }
  }

  /**
   * Given a function call node, we have to do a two pass processing of the 
   * function arguments in order to properly handle any potential lambda functions.
   * In the first pass, if there are lambda functions, we create a SqlLambda that only contains
   * the number of input arguments for the lambda. We pass this first argument list
   * to UdfFactory in order to get the correct function. We can make this assumption 
   * due to Java's handling of type erasure (Function(T,R) is considered the same as
   * Function(U,R)).
   * In the second pass, we use the LambdaType inputTypes field to construct SqlLambdaResolved
   * that has the proper input type list and return type. We also need to construct a list of
   * contexts that should be used when processing each function argument subtree.
   *
   * @param expressionTypeManager an expression type manager
   * @param functionCall  the function expression
   * @param udfFactory  udf factory for the function in the expression
   * @param context a type context
   *
   * @return a wrapper that contains a list of function arguments 
   *         (any lambdas are SqlLambdaResolved), the ksql function, 
   *         type contexts for use in further processing the function 
   *         argument child nodes, and the return type of the ksql function
   */
  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  public static FunctionArgumentsAndContext getLambdaContextAndType(
      final ExpressionTypeManager expressionTypeManager,
      final FunctionCall functionCall,
      final UdfFactory udfFactory,
      final TypeContext context
  ) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    final List<Expression> arguments = functionCall.getArguments();
    final List<SqlArgument> functionArgumentTypes = firstPassOverFunctionArguments(
        arguments,
        expressionTypeManager,
        context
    );

    final KsqlScalarFunction function = udfFactory.getFunction(functionArgumentTypes);
    final SqlType returnSchema;
    final List<TypeContext> typeContextsForChildren = new ArrayList<>();

    if (!functionCall.hasLambdaFunctionCallArguments()) {
      returnSchema = function.getReturnType(functionArgumentTypes);
      arguments.forEach(argument -> typeContextsForChildren.add(context.getCopy()));
      return new FunctionArgumentsAndContext(
          typeContextsForChildren,
          functionArgumentTypes,
          returnSchema,
          function
      );
    } else {
      final List<ParamType> paramTypes = function.parameters();
      final Map<GenericType, SqlType> reservedGenerics = new HashMap<>();
      final List<SqlArgument> functionArgumentTypesWithResolvedLambdaType = new ArrayList<>();

      // second pass over the function arguments to properly do lambda type checking
      for (int i = 0; i < arguments.size(); i++) {
        final Expression expression = arguments.get(i);
        final ParamType parameter = paramTypes.get(i);
        if (expression instanceof LambdaFunctionCall) {
          final TypeContext childContext = context.getCopy();

          // the function returned from the UDF factory should have lambda
          // at this index in the function arguments if there's a
          // lambda node at this index in the function node argument list
          if (!(parameter instanceof LambdaType)) {
            throw new RuntimeException("Error while processing lambda function. "
                + "This is most likely an internal error and a "
                + "Github issue should be filed for debugging. "
                + "Include the function name, the parameters passed in, the expected "
                + "signature, and any other relevant information.");
          }

          final LambdaType lambdaParameter = (LambdaType) parameter;
          for (ParamType inputParam : lambdaParameter.inputTypes()) {
            if (inputParam instanceof GenericType) {
              final GenericType genericParam = (GenericType) inputParam;
              if (!reservedGenerics.containsKey(genericParam)) {
                throw new RuntimeException(
                    String.format(
                        "Could not resolve type for generic %s. "
                            + "The generic mapping so far: %s",
                        genericParam.toString(), reservedGenerics.toString()));
              }
              childContext.addLambdaInputType(reservedGenerics.get(genericParam));
            } else {
              childContext.addLambdaInputType(
                  SchemaConverters.functionToSqlConverter().toSqlType(inputParam));
            }
          }

          final SqlType resolvedLambdaReturnType =
              expressionTypeManager.getExpressionSqlType(expression, childContext.getCopy());
          final SqlArgument lambdaArgument = SqlArgument.of(
              SqlLambdaResolved.of(childContext.getLambdaInputTypes(), resolvedLambdaReturnType));

          functionArgumentTypesWithResolvedLambdaType.add(lambdaArgument);
          typeContextsForChildren.add(childContext);
        } else {
          functionArgumentTypesWithResolvedLambdaType.add(functionArgumentTypes.get(i));
          typeContextsForChildren.add(context.getCopy());
        }

        if (GenericsUtil.hasGenerics(parameter)) {
          final Pair<Boolean, Optional<KsqlException>> success = GenericsUtil.reserveGenerics(
              parameter,
              functionArgumentTypesWithResolvedLambdaType.get(i),
              reservedGenerics
          );
          if (!success.getLeft() && success.getRight().isPresent()) {
            throw success.getRight().get();
          }
        }
      }

      returnSchema = function.getReturnType(functionArgumentTypesWithResolvedLambdaType);
      return new FunctionArgumentsAndContext(
          typeContextsForChildren,
          functionArgumentTypesWithResolvedLambdaType,
          returnSchema,
          function
      );
    }
  }
  
  private static List<SqlArgument> firstPassOverFunctionArguments(
      final List<Expression> arguments,
      final ExpressionTypeManager expressionTypeManager,
      final TypeContext context
  ) {
    final List<SqlArgument> functionArgumentTypes = new ArrayList<>();
    for (final Expression expression : arguments) {
      if (expression instanceof LambdaFunctionCall) {
        functionArgumentTypes.add(
            SqlArgument.of(
                SqlLambda.of(((LambdaFunctionCall) expression).getArguments().size())));
      } else {
        final TypeContext childContext = context.getCopy();
        expressionTypeManager.getExpressionSqlType(expression, childContext);
        final SqlType resolvedArgType = childContext.getSqlType();
        functionArgumentTypes.add(SqlArgument.of(resolvedArgType));
      }
    }
    return functionArgumentTypes;
  }
}
