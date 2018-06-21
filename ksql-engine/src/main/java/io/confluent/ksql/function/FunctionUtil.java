/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function;

public final class FunctionUtil {

  private FunctionUtil() {

  }

  /**
   * Given the arguments and types for a function ensures the args are correct type.
   *
   * @param functionName The name of the function
   * @param args Argument array
   * @param argTypes Expected argument types
   */
  public static void ensureCorrectArgs(
      final String functionName,
      final Object[] args,
      final Class... argTypes) {

    if (args == null) {
      throw new KsqlFunctionException(String.format("Null argument list for %s.", functionName));
    }

    if (args.length != argTypes.length) {
      throw new KsqlFunctionException(String.format("Incorrect arguments for %s.", functionName));
    }

    for (int i = 0; i < argTypes.length; i++) {
      if (args[i] == null) {
        continue;
      }
      if (args[i].getClass() != argTypes[i]) {
        throw new KsqlFunctionException(
            String.format("Inorrect arguments type for %s. "
                + "Expected %s for arg number %d but found %s.",
                functionName,
                argTypes[i].getCanonicalName(),
                i,
                args[i].getClass().getCanonicalName()));
      }
    }
  }
}
