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

import java.util.function.Function;

/**
 * Helper functions around null handling
 */
public final class NullSafe {

  private NullSafe() {
  }

  /**
   * Helper to guard a non-null-safe {@code mapper} function.
   *
   * @param input the input value.
   * @param mapper the mapper to call if non-null.
   * @param <I> the input type
   * @param <O> the output type
   * @return the return value of the mapper, or {@code null}.
   */
  public static <I, O> O apply(final I input, final Function<I, O> mapper) {
    return applyOrDefault(input, mapper, null);
  }

  /**
   * Helper to guard a non-null-safe {@code mapper} function, or
   * {@code defaultValue} if the value is {@code null}.
   *
   * @param input the input value.
   * @param mapper the mapper to call if non-null.
   * @param defaultValue the value to return if null.
   * @param <I> the input type
   * @param <O> the output type
   * @return the return value of the mapper, or {@code defaultValue}.
   */
  public static <I, O> O applyOrDefault(
      final I input,
      final Function<I, O> mapper,
      final O defaultValue
  ) {
    if (input == null) {
      return defaultValue;
    }

    return mapper.apply(input);
  }

  /**
   * Generate the code for wrapping a non-null-safe {@code lambdaBody} in a null-safe {@link #apply}
   * call.
   *
   * @param inputCode the input code.
   * @param mapperCode the mapper code.
   * @param returnType the type the mapper returns.
   * @return the code to wrap the mapper in {@link #apply}
   */
  public static String generateApply(
      final String inputCode,
      final String mapperCode,
      final Class<?> returnType
  ) {
    return "((" + returnType.getSimpleName() + ")" + NullSafe.class.getSimpleName()
        + ".apply(" + inputCode + "," + mapperCode + "))";
  }

  /**
   * Generate the code for wrapping a non-null-safe {@code lambdaBody} in a null-safe
   * {@link #applyOrDefault} call.
   *
   * @param inputCode the input code.
   * @param mapperCode the mapper code.
   * @param defaultValueCode the default value code.
   * @param returnType the type the mapper returns.
   * @return the code to wrap the mapper in {@link #applyOrDefault}
   */
  public static String generateApplyOrDefault(
      final String inputCode,
      final String mapperCode,
      final String defaultValueCode,
      final Class<?> returnType
  ) {
    return "(" + returnType.getSimpleName() + ")" + NullSafe.class.getSimpleName()
        + ".applyOrDefault(" + inputCode + "," + mapperCode + ", " + defaultValueCode + ")";
  }
}
