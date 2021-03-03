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

import com.google.common.collect.ImmutableMap;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class FunctionArgumentsUtil {

  private FunctionArgumentsUtil() {

  }

  public static final class FunctionTypeInfo {
    private final List<ArgumentInfo> argumentInfos;
    private final SqlType returnType;
    private final KsqlScalarFunction function;

    private static FunctionTypeInfo of(
        final List<ArgumentInfo> argumentInfos,
        final SqlType returnType,
        final KsqlScalarFunction function
    ) {
      return new FunctionTypeInfo(argumentInfos, returnType, function);
    }

    private FunctionTypeInfo(
        final List<ArgumentInfo> argumentInfos,
        final SqlType returnType,
        final KsqlScalarFunction function
    ) {
      this.argumentInfos = argumentInfos;
      this.returnType = returnType;
      this.function = function;
    }

    public List<ArgumentInfo> getArgumentInfos() {
      return argumentInfos;
    }

    public SqlType getReturnType() {
      return returnType;
    }

    public KsqlScalarFunction getFunction() {
      return function;
    }
  }

  public static final class ArgumentInfo {
    final SqlArgument sqlArgument;
    final TypeContext context;

    private static ArgumentInfo of(
        final SqlArgument sqlArgument,
        final TypeContext context
    ) {
      return new ArgumentInfo(sqlArgument, context);
    }

    private ArgumentInfo(
        final SqlArgument sqlArgument,
        final TypeContext context
    ) {
      this.sqlArgument = sqlArgument;
      this.context = context;
    }

    public SqlArgument getSqlArgument() {
      return sqlArgument;
    }

    public TypeContext getContext() {
      return context;
    }
  }

  /**
   * Compute type information given a function call node. Specifically, computes
   * the function return type, the types of all arguments, and the types of all
   * lambda parameters for arguments that are lambda expressions.
   *
   * <p>Given a function call node, we have to do a two pass processing of the
   * function arguments in order to properly handle any potential lambda functions.</p>
   *
   * <p>In the first pass, if there are lambda functions, we create a SqlLambda that only contains
   * the number of input arguments for the lambda. We pass this first argument list
   * to UdfFactory in order to get the correct function. We can make this assumption 
   * due to Java's handling of type erasure (Function(T,R) is considered the same as
   * Function(U,R)).</p>
   *
   * <p>In the second pass, we use the LambdaType inputTypes field to construct SqlLambdaResolved
   * that has the proper input type list and return type. We also need to construct a list of
   * contexts that should be used when processing each function argument subtree.</p>
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
  public static FunctionTypeInfo getFunctionTypeInfo(
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
      return FunctionTypeInfo.of(
          functionArgumentTypes.stream()
              .map(argument -> ArgumentInfo.of(argument, context.getCopy()))
              .collect(Collectors.toList()),
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
          // the function returned from the UDF factory should have lambda
          // at this index in the function arguments if there's a
          // lambda node at this index in the function node argument list
          if (!(parameter instanceof LambdaType)) {
            throw new RuntimeException(String.format("Error while processing lambda function."
                + "Expected lambda parameter but was %s"
                + "This is most likely an internal error and a "
                + "Github issue should be filed for debugging. "
                + "Include the function name, the parameters passed in, the expected "
                + "signature, and any other relevant information.", parameter.toString()));
          }

          final ArrayList<SqlType> lambdaSqlTypes = new ArrayList<>();
          final Map<String, SqlType> variableTypeMapping = mapLambdaParametersToTypes(
              (LambdaFunctionCall) expression,
              (LambdaType) parameter,
              reservedGenerics,
              lambdaSqlTypes
          );

          final TypeContext childContext =
              context.copyWithLambdaVariableTypeMapping(variableTypeMapping);

          final SqlType resolvedLambdaReturnType =
              expressionTypeManager.getExpressionSqlType(expression, childContext.getCopy());
          final SqlArgument lambdaArgument = SqlArgument.of(
              SqlLambdaResolved.of(lambdaSqlTypes, resolvedLambdaReturnType));

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
      return new FunctionTypeInfo(
          IntStream.range(0, functionArgumentTypesWithResolvedLambdaType.size())
              .mapToObj(i -> ArgumentInfo.of(
                  functionArgumentTypesWithResolvedLambdaType.get(i),
                  typeContextsForChildren.get(i)))
              .collect(Collectors.toList()),
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

  private static Map<String, SqlType> mapLambdaParametersToTypes(
      final LambdaFunctionCall lambdaFunctionCall,
      final LambdaType lambdaParameter,
      final Map<GenericType, SqlType> reservedGenerics,
      final ArrayList<SqlType> lambdaSqlTypes
  ) {
    if (lambdaFunctionCall.getArguments().size() != lambdaParameter.inputTypes().size()) {
      throw new IllegalArgumentException("Was expecting "
          + lambdaParameter.inputTypes().size()
          + " arguments but found "
          + lambdaFunctionCall.getArguments().size() + ", "
          + lambdaFunctionCall.getArguments()
          + ". Check your lambda statement.");
    }

    final Iterator<String> lambdaArgs = lambdaFunctionCall.getArguments().listIterator();
    final HashMap<String, SqlType> variableTypeMapping = new HashMap<>();
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
        variableTypeMapping.put(lambdaArgs.next(), reservedGenerics.get(genericParam));
        lambdaSqlTypes.add(reservedGenerics.get(genericParam));
      } else {
        variableTypeMapping.put(
            lambdaArgs.next(),
            SchemaConverters.functionToSqlConverter().toSqlType(inputParam)
        );
        lambdaSqlTypes.add(SchemaConverters.functionToSqlConverter().toSqlType(inputParam));
      }
    }
    return ImmutableMap.copyOf(variableTypeMapping);
  }
}
