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

import static java.util.Objects.requireNonNull;

/**
 * Checks to see if the line is in the middle of a quote.  Unlike
 * org.jline.reader.impl.DefaultParser, this is comment aware.
 */
public final class UnclosedQuoteChecker {
  private static final String COMMENT = "--";

  private UnclosedQuoteChecker() {}

  public static boolean isUnclosedQuote(final String line) {
    requireNonNull(line, "line");
    int quoteStart = -1;
    for (int i = 0; line != null && i < line.length(); ++i) {
      if (quoteStart < 0 && isQuoteChar(line, i)) {
        quoteStart = i;
      } else if (quoteStart >= 0 && line.charAt(quoteStart) == line.charAt(i)
          && !isEscaped(line, i)) {
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
    if (c == '\\') {
      return true;
    }
    return false;
  }
}
