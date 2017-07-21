/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtil {
  public static String stackTraceToString(Exception e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    return sw.toString();
  }
}
