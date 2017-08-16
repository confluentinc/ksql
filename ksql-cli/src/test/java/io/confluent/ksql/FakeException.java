/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql;

public class FakeException extends RuntimeException {

  @Override
  public String getMessage() {
    return "This Exception is only used for verifying exception prints. It doesn't mean anything goes wrong.";
  }

  @Override
  public StackTraceElement[] getStackTrace() {
    return new StackTraceElement[0];
  }
}
