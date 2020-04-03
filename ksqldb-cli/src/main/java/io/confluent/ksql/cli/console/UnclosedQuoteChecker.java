package io.confluent.ksql.cli.console;

/**
 * Checks to see if the line is in the middle of a quote.  Unlike
 * org.jline.reader.impl.DefaultParser, this is comment aware.
 */
public class UnclosedQuoteChecker {
  private static final String COMMENT = "--";

  public static boolean isUnclosedQuote(String line) {
    int quoteStart = -1;
    for(int i = 0; line != null && i < line.length(); ++i) {
      if (quoteStart < 0 && isQuoteChar(line, i)) {
        quoteStart = i;
      } else if (quoteStart >= 0 && line.charAt(quoteStart) == line.charAt(i) &&
          !isEscaped(line, i)) {
        quoteStart = -1;
      }
    }
    int commentInd = line.indexOf(COMMENT);
    if (commentInd < 0) {
      return quoteStart >= 0;
    } else if (quoteStart < 0) {
      return false;
    } else {
      return commentInd > quoteStart;
    }
  }

  private static boolean isQuoteChar(String line, int ind) {
    char c = line.charAt(ind);
    if (c == '\'') {
      return true;
    }
    return false;
  }

  private static boolean isEscaped(String line, int ind) {
    if (ind == 0) {
      return false;
    }
    char c = line.charAt(ind-1);
    if (c == '\\') {
      return true;
    }
    return false;
  }
}
