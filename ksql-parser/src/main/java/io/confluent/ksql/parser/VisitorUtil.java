/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.parser;

public final class VisitorUtil {
  private VisitorUtil() {
  }

  public static RuntimeException unsupportedOperation(
      final Object visitor,
      final Object obj) {
    return new UnsupportedOperationException(
        String.format(
            "not yet implemented: %s.visit%s",
            visitor.getClass().getName(),
            obj.getClass().getSimpleName()
        )
    );
  }

  public static RuntimeException illegalState(
      final Object visitor,
      final Object obj) {
    return new IllegalStateException(
        String.format(
            "Type %s should never be visited by %s",
            obj.getClass(),
            visitor.getClass()));
  }
}
