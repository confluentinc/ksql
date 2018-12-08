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
 */

package io.confluent.ksql.cli.console;

/**
 * Strips single line comments of the end of a line.
 *
 * <p>i.e. anything following an unquoted {@code --}.
 */
final class CommentStripper {

  private static final char NONE = 'x';

  private CommentStripper() {
  }

  static String strip(final String line) {

    char lastChar = NONE;
    char lastQuote = NONE;
    for (int i = 0; i != line.length(); ++i) {
      final char c = line.charAt(i);
      switch (c) {
        case '`':
        case '\'':
        case '"':
          if (lastQuote == c) {
            // Matching pair:
            lastQuote = NONE;
          } else if (lastQuote == NONE) {
            lastQuote = c;
          }
          break;

        case '-':
          if (lastChar == '-' && lastQuote == NONE) {
            // Found unquoted comment marker:
            return line.substring(0, i - 1).trim();
          }
          break;

        default:
          break;
      }

      lastChar = c;
    }

    return line;
  }
}
