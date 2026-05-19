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

package io.confluent.ksql.execution.codegen.helpers;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.List;

/**
 * Functions to help with generating code to work around the fact that the script engine doesn't
 * support lambdas.
 */
public final class LambdaUtil {

  private LambdaUtil() {
  }

  /**
   * Generate code to build a {@link java.util.function.Function}.
   *
   * @param argName the name of the single argument the  {@code lambdaBody} expects.
   * @param argType the type of the single argument the {@code lambdaBody} expects.
   * @param lambdaBody the body of the lambda.
   * @return code to instantiate the function.
   */
  public static String toJavaCode(
      final String argName,
      final Class<?> argType,
      final String lambdaBody
  ) {
    return toJavaCode(ImmutableList.of(new Pair<>(argName, argType)), lambdaBody);
  }

  /**
   * Generate code to build a {@link java.util.function.Function}.
   *
   * @param argList a list of lambda arguments that the {@code lambdaBody} expects.
   *                The type is paired with each argument.
   * @param lambdaBody the body of the lambda.
   * @return code to instantiate the function.
   */
  // CHECKSTYLE_RULES.OFF: FinalLocalVariable
  public static String toJavaCode(
      final List<Pair<String, Class<?>>> argList,
      final String lambdaBody
  ) {
    final StringBuilder arguments = new StringBuilder();
    int i = 0;
    for (final Pair<String, Class<?>> argPair : argList) {
      i++;
      final String javaType = argPair.right.getSimpleName();
      arguments.append(
          "   " + "final" + " " + javaType + " " + argPair.left
              + " = (" + javaType + ") arg" + i + ";\n");
    }
    String functionType;
    String functionApply;
    if (argList.size() == 1) {
      functionType = "Function()";
      functionApply = " public Object apply(Object arg1) {\n";
    } else if (argList.size() == 2) {
      functionType = "BiFunction()";
      functionApply = " public Object apply(Object arg1, Object arg2) {\n";
    } else if (argList.size() == 3) {
      functionType = "TriFunction()";
      functionApply = " public Object apply(Object arg1, Object arg2, Object arg3) {\n";
    } else {
      throw new KsqlException("Unsupported number of lambda arguments.");
    }
    
    final String function =  "new " + functionType + " {\n"
        + " @Override\n"
        + functionApply
        + arguments.toString()
        + "   return " + lambdaBody + ";\n"
        + " }\n"
        + "}";
    return function;
  }
}
