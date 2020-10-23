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

package io.confluent.ksql.util;

/**
 * Util class for working with SQL identifiers
 */
public final class Identifiers {

  private Identifiers() {
  }

  /**
   * Given a user supplied identifier, return the SQL identifier text.
   *
   * <p><b>This method does not validate unquoted identifiers are valid SQL identifiers.</b> See
   * {@code IdentifierUtil} in the parser module for such functionality.
   *
   * <p>User supplied identifier can be quoted or unquoted.
   *
   * <p>Unquoted identifiers are converted to UPPERCASE.
   *
   * <p>Quoted identifiers retain their case, and can contain escaped quotes.
   *
   * @param text user supplied identifier.
   * @return unquoted identifier text.
   */
  public static String getIdentifierText(final String text) {
    if (text.isEmpty()) {
      return "";
    }

    final char firstChar = text.charAt(0);
    if (firstChar == '`' || firstChar == '"') {
      return validateAndUnquote(text, firstChar);
    }

    return text.toUpperCase();
  }

  private static String validateAndUnquote(final String value, final char quote) {
    if (value.charAt(0) != quote) {
      throw new IllegalStateException("Value must begin with quote");
    }
    if (value.charAt(value.length() - 1) != quote || value.length() < 2) {
      throw new IllegalArgumentException("Expected matching quote at end of value");
    }

    int i = 1;
    while (i < value.length() - 1) {
      if (value.charAt(i) == quote) {
        if (value.charAt(i + 1) != quote || i + 1 == value.length() - 1) {
          throw new IllegalArgumentException("Un-escaped quote in middle of value at index " + i);
        }
        i += 2;
      } else {
        i++;
      }
    }

    return value.substring(1, value.length() - 1)
        .replace("" + quote + quote, "" + quote);
  }
}
