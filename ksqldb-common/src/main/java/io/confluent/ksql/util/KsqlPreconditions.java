/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

public final class KsqlPreconditions {

  private KsqlPreconditions() {
  }

  /**
   * Ensures the truth of an expression involving one or more parameters to the calling method.
   *
   * @param expression a boolean expression
   * @throws KsqlException if {@code expression} is false
   */
  public static void checkArgument(final boolean expression, final String message) {
    if (!expression) {
      throw new KsqlException(message);
    }
  }

  /**
   * Ensures the truth of an expression involving one or more parameters to the calling method.
   *
   * @param expression a boolean expression
   * @throws KsqlServerException if {@code expression} is false
   */
  public static void checkServerCondition(final boolean expression, final String message) {
    if (!expression) {
      throw new KsqlServerException(message);
    }
  }

}
