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

package io.confluent.ksql.cli.console;

/**
 * Checks to see if the line is in the middle of a quote.  Unlike
 * org.jline.reader.impl.DefaultParser, this is comment aware.
 */
public final class UnclosedQuoteChecker {
  private static final String COMMENT = "--";

  private UnclosedQuoteChecker() {

  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  public static boolean isUnclosedQuote(final String line) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    int quoteStart = -1;
    for (int i = 0; i < line.length(); ++i) {
      if (quoteStart < 0 && isQuoteChar(line, i)) {
        quoteStart = i;
      } else if (quoteStart >= 0 && isTwoQuoteStart(line, i) && !isEscaped(line, i)) {
        // Together, two quotes are effectively an escaped quote and don't act as a quote character.
        // Skip the next quote char, since it's coupled with the first.
        i++;
      } else if (quoteStart >= 0 && isQuoteChar(line, i) && !isEscaped(line, i)) {
        quoteStart = -1;
      }
    }
    final int commentInd = line.indexOf(COMMENT);
    if (commentInd < 0) {
      return quoteStart >= 0;
    } else if (quoteStart < 0) {
      return false;
    } else {
      return commentInd > quoteStart;
    }
  }

  private static boolean isQuoteChar(final String line, final int ind) {
    final char c = line.charAt(ind);
    if (c == '\'') {
      return true;
    }
    return false;
  }

  private static boolean isEscaped(final String line, final int ind) {
    if (ind == 0) {
      return false;
    }
    final char c = line.charAt(ind - 1);
    if (c == '\\' && !isEscaped(line, ind - 1)) {
      return true;
    }
    return false;
  }

  // Technically, it is sufficient to ensure that quotes are paired up to answer the "unclosed"
  // question.  It's still clearer to differentiate between the two-quote escaped quote and two
  // back-to-back strings (e.g. 'ab''cd' is really evaluated as "ab'cd" rather than 'ab' and 'cd'),
  // so we explicitly check for that case here.
  private static boolean isTwoQuoteStart(final String line, final int ind) {
    if (ind + 1 >= line.length()) {
      return false;
    }
    return isQuoteChar(line, ind) && isQuoteChar(line, ind + 1);
  }
}
