/*
 * Copyright 2019 Confluent Inc.
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

public final class SandboxUtil {

  private SandboxUtil() {

  }

  /**
   * @param object the object to check
   * @return the object that was passed in
   *
   * @throws IllegalArgumentException if {@code object} is not an instance of a class that
   *                                  is annotated with {@link Sandbox}
   */
  public static <T> T requireSandbox(final T object) {
    if (object.getClass().isAnnotationPresent(Sandbox.class)) {
      return object;
    }
    throw new IllegalArgumentException("Expected sandbox but got: " + object.getClass());
  }

}
