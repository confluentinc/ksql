/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
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
    return new Parser(line).parse();
  }

  private static final class Parser {
    private final String line;
    private int pos;

    private Parser(final String line) {
      this.line = line;
      this.pos = 0;
    }

    private String parse() {
      final String firstPart = strip();
      if (done()) {
        // Early out if contains no comments part way through:
        return firstPart;
      }

      final StringBuilder builder = new StringBuilder(line.length());
      builder.append(firstPart);
      while (!done()) {
        builder.append(strip());
      }
      return builder.toString();
    }

    private boolean done() {
      return pos == line.length();
    }

    private String strip() {
      final int start = pos;
      char lastChar = NONE;
      char lastQuote = NONE;
      for (; pos != line.length(); ++pos) {
        final char c = line.charAt(pos);

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
              return trimComment(start);
            }
            break;

          default:
            break;
        }

        lastChar = c;
      }

      return line.substring(start);
    }

    private String trimComment(final int partStart) {
      final String part = line.substring(partStart, pos - 1).trim();

      final int newLine = line.indexOf(System.lineSeparator(), pos + 1);
      if (newLine == -1) {
        pos = line.length();
      } else {
        pos = newLine;
      }

      return part;
    }
  }
}
