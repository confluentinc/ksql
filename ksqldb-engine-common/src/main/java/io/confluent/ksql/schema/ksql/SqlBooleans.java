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

package io.confluent.ksql.schema.ksql;

import java.util.Optional;

/**
 * Helpers for working with Sql {@code BOOLEAN}.
 */
public final class SqlBooleans {

  private SqlBooleans() {
  }

  /**
   * Parse a SQL boolean from a string, treating any non true value as {@code false}.
   *
   * <p>The function returns {@code true} if the string is (case-insensitive) either {@code 'yes'}
   * or {@code 'true'} or a substring of either, otherwise {@code false}.
   *
   * @param value the value to parse
   * @return the boolean parsed from the string.
   */
  public static boolean parseBoolean(final String value) {
    return booleanStringMatches(value, true);
  }

  /**
   * Parse a SQL boolean from a string, throwing if the string does ot contain a valid boolean.
   *
   * <p>The function returns {@code true} if the string is (case-insensitive) either {@code 'yes'}
   * or {@code 'true'} or a substring of either, {@code false} if the string is (case-insensitive)
   * either {@code 'no'} or {@code 'false'} or a substring of either, otherwise {@code empty}.
   *
   * @param value the value to parse
   * @return the boolean parsed from the string.
   * @throws IllegalArgumentException if the string does not contain a boolean value.
   */
  public static Optional<Boolean> parseBooleanExact(final String value) {
    if (booleanStringMatches(value, true)) {
      return Optional.of(true);
    }

    if (booleanStringMatches(value, false)) {
      return Optional.of(false);
    }

    return Optional.empty();
  }

  private static boolean booleanStringMatches(final String str, final boolean required) {
    if (str.isEmpty()) {
      return false;
    }

    final String tf = required ? "true" : "false";
    final String yn = required ? "yes" : "no";
    return str.equalsIgnoreCase(tf.substring(0, Math.min(str.length(), tf.length())))
        || str.equalsIgnoreCase(yn.substring(0, Math.min(str.length(), yn.length())));
  }
}
