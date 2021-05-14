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

package io.confluent.ksql.parser;

import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.util.KsqlException;

/**
 * Utility class for working with {@link Literal} properties.
 */
public final class LiteralUtil {

  private LiteralUtil() {
  }

  public static boolean toBoolean(final Literal literal, final String propertyName) {
    final String value = literal.getValue().toString();
    final boolean isTrue = value.equalsIgnoreCase("true");
    final boolean isFalse = value.equalsIgnoreCase("false");
    if (!isTrue && !isFalse) {
      throw new KsqlException("Property '" + propertyName + "' is not a boolean value");
    }
    return isTrue;
  }
}
