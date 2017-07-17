/**
* Copyright 2017 Confluent Inc.
**/

package io.confluent.ksql.util;

public class StringUtil {
  public static String cleanQuotes(final String stringWithQuotes) {
    // TODO: move check to grammer
    if (stringWithQuotes.startsWith("'") && stringWithQuotes.endsWith("'")) {
      return stringWithQuotes.substring(1, stringWithQuotes.length() - 1);
    } else {
      throw new KsqlException(stringWithQuotes
          + " value is string and should be enclosed between "
          + "' .");
    }
  }
}
