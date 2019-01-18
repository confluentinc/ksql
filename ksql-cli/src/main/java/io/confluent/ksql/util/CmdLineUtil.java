/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableList;
import java.util.List;

public final class CmdLineUtil {

  private CmdLineUtil() {
  }

  /**
   * Split the supplied line by unquoted whitespace.
   *
   * <p>White space within an open single quote will be ignored
   *
   * <p>A quote can be escaped using a second single quote, e.g. {@code "'escaped '' quote'"}.
   *
   * <ul>
   * <li>{@code "t0 t1"} becomes {@code ["t0", "t1"]}</li>
   * <li>{@code "'quoted string'"} becomes {@code ["quoted string"]}</li>
   * <li>{@code "'quoted'connected'quoted'"} becomes {@code ["quotedconnectedquoted"]}</li>
   * <li>{@code "'escaped '' quote'"} becomes {@code ["escaped ' quote"]}</li>
   * </ul>
   *
   * @return the list of parts.
   */
  public static List<String> splitByUnquotedWhitespace(final String string) {
    return new WhitespaceParser().parse(string);
  }

  /**
   * Remove any matched, i.e. closed, single quotes from the supplied {@code string}.
   *
   * <p>Works in a similar way to a unix command line would.
   *
   * <p>A quote can be escaped using a second single quote, e.g. {@code "'escaped '' quote'"}.
   *
   * <p>e.g.
   * <ul>
   * <li>{@code "unquoted"} becomes {@code "unquoted"}</li>
   * <li>{@code "'some quoted string'"} becomes {@code "some quoted string"}</li>
   * <li>{@code "'quoted'connected'quoted'"} becomes {@code "quotedconnectedquoted"}</li>
   * <li>{@code "'escaped '' quote'"} becomes {@code "escaped ' quote"}</li>
   * </ul>
   *
   * @return the input with matched quotes removed.
   */
  public static String removeMatchedSingleQuotes(final String string) {
    return new QuoteRemover().parse(string);
  }

  private static final class WhitespaceParser {

    private boolean inQuotes;
    private boolean inWhitespace;
    private String input;
    private ImmutableList.Builder<String> output;
    private final StringBuilder currentToken = new StringBuilder();

    private List<String> parse(final String s) {
      inQuotes = false;
      inWhitespace = false;
      input = s.trim();
      currentToken.setLength(0);
      output = ImmutableList.builder();

      for (int pos = 0; pos != input.length(); ++pos) {
        pos = processCharAt(pos);
      }

      final String remainder = currentToken.toString();
      if (!remainder.isEmpty()) {
        output.add(remainder);
      }

      return output.build();
    }

    private int processCharAt(final int pos) {
      final char c = input.charAt(pos);
      int returnPos = pos;

      if (!Character.isWhitespace(c)) {
        inWhitespace = false;
      } else if (!inQuotes && !inWhitespace) {
        inWhitespace = true;

        output.add(currentToken.toString());
        currentToken.setLength(0);
      }

      if (!inWhitespace) {
        currentToken.append(c);
      }

      if (c == '\'') {
        if (!inQuotes) {
          inQuotes = true;
        } else if (isEscapedChar(input, pos)) {
          ++returnPos;
        } else {
          inQuotes = false;
        }
      }
      return returnPos;
    }
  }

  private static final class QuoteRemover {
    private static final int NOT_SET = -1;
    private int quoteStart;
    private int unquotedStart;
    private String input;
    private String result;

    private String parse(final String s) {
      quoteStart = NOT_SET;
      unquotedStart = NOT_SET;
      input = s;
      result = "";

      for (int pos = 0; pos != input.length(); ++pos) {
        pos = processCharAt(pos);
      }

      if (quoteStart != NOT_SET) {
        result += input.substring(quoteStart);
      } else if (unquotedStart != NOT_SET) {
        result += input.substring(unquotedStart);
      }

      return result;
    }

    private int processCharAt(final int pos) {
      final char c = input.charAt(pos);
      int returnPos = pos;

      if (c == '\'') {
        if (quoteStart == NOT_SET) {
          if (unquotedStart != NOT_SET) {
            appendResult(unquotedStart, pos);
            unquotedStart = NOT_SET;
          }

          quoteStart = pos;
        } else if (isEscapedChar(input, pos)) {
          ++returnPos;
        } else {
          appendResult(quoteStart + 1, pos);
          quoteStart = NOT_SET;
        }
      } else if (quoteStart == NOT_SET) {
        if (unquotedStart == NOT_SET) {
          unquotedStart = pos;
        }
      }
      return returnPos;
    }

    private void appendResult(final int start, final int end) {
      result += input.substring(start, end).replaceAll("''", "'");
    }
  }

  private static boolean isEscapedChar(final String input, final int pos) {
    return input.length() >= pos + 2 && input.charAt(pos + 1) == '\'';
  }
}
