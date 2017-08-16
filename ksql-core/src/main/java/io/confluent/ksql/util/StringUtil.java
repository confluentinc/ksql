/**
* Copyright 2017 Confluent Inc.
**/

package io.confluent.ksql.util;

import java.util.List;

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

  public static String join(String delimiter, List<? extends Object> objs) {
    StringBuilder sb = new StringBuilder();
    int cnt = 0;
    for (Object obj : objs) {
      if (cnt > 0) {
        sb.append(delimiter);
      }
      sb.append(obj);
      cnt += 1;
    }
    return sb.toString();
  }

}
