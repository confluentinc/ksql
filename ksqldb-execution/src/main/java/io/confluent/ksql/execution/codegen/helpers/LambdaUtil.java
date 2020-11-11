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
   * @param lambdaBody the body of the lambda. It will find the n
   * @return code to instantiate the function.
   */
  public static String function(
      final String argName,
      final Class<?> argType,
      final String lambdaBody
  ) {
    final String javaType = argType.getSimpleName();
    return "new Function() {\n"
        + " @Override\n"
        + " public Object apply(Object arg) {\n"
        + "   " + javaType + " " + argName + " = (" + javaType + ") arg;\n"
        + "   return  " + lambdaBody + ";\n"
        + " }\n"
        + "}";
  }
}
