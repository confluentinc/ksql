/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.exception;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtil {
  public static String stackTraceToString(Exception e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    return sw.toString();
  }

  public static String getCurrentStackTraceToString() {
    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
    StringBuilder sb = new StringBuilder();
    for (StackTraceElement element : stackTraceElements) {
      sb.append(element.toString());
      sb.append("\n");
    }
    return sb.toString();
  }
}
