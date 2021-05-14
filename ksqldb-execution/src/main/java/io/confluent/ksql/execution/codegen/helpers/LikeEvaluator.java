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

package io.confluent.ksql.execution.codegen.helpers;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Used in the code generation to evaluate SQL 'LIKE' expressions.
 *
 * @see io.confluent.ksql.execution.expression.tree.LikePredicate
 */
public final class LikeEvaluator {

  private LikeEvaluator() {
  }

  /**
   * @param val     the value to match
   * @param pattern the pattern to match against
   *
   * @return  whether or not {@code val} matches {@code pattern} given the SQL
   *          LIKE definition of matching
   */
  public static boolean matches(
      final String val,
      final String pattern
  ) {
    return matches(val, pattern, Optional.empty());
  }

  /**
   * @param val     the value to match
   * @param pattern the pattern to match against
   * @param escape  the escape character, if any
   *
   * @return  whether or not {@code val} matches {@code pattern} given the SQL
   *          LIKE definition of matching
   */
  public static boolean matches(
      final String val,
      final String pattern,
      final char escape
  ) {
    return matches(val, pattern, Optional.of(escape));
  }

  @VisibleForTesting
  static boolean matches(
      final String val,
      final String pattern,
      final Optional<Character> escape
  ) {
    final StringBuilder regex = new StringBuilder();

    final char[] chars = pattern.toCharArray();

    boolean escaped = false;
    int start = 0; // start of a plain-text portion of the pattern
    int i = 0;     // the current index, also doubles as the end of a plain-text portion

    for (; i < chars.length; i++) {
      final char c = chars[i];

      if (escaped) {
        escaped = false;
      } else if (escape.filter(e -> e == c).isPresent()) {
        // the escape character is treated differently in different databases
        // the only SQL standard indicates that
        //
        //    "A wildcard character is treated as a literal if preceded by
        //    the escape character."
        //
        // This implementation will assume that if an escape character is encountered,
        // the next character will be treated as a literal. Furthermore, if the escape
        // character is one of the special characters ('%' or '_') then it becomes
        // impossible to use those characters for their special meaning
        //
        // To use the escape character in the pattern, have it precede itself
        escaped = true;
        append(pattern, start, i, regex);
        start = i + 1;
      } else if (c == '%') {
        append(pattern, start, i, regex);
        regex.append(".*");
        start = i + 1;
      } else if (c == '_') {
        append(pattern, start, i, regex);
        regex.append(".");
        start = i + 1;
      }
    }

    if (escaped) {
      throw new KsqlException("LIKE pattern must not end with escape character");
    }

    append(pattern, start, i, regex);
    return Pattern.compile(regex.toString()).matcher(val).matches();
  }

  /**
   * Escapes a plain-text portion of the pattern matching expression
   * so that any non-special SQL characters don't translate into special
   * regex characters.
   */
  private static void append(
      final String pattern,
      final int start,
      final int end,
      final StringBuilder regex
  ) {
    if (end - start > 0) {
      regex.append(Pattern.quote(pattern.substring(start, end)));
    }
  }

}
