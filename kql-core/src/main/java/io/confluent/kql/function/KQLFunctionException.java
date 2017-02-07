/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.function;

import io.confluent.kql.util.KQLException;

public class KQLFunctionException extends KQLException {

  public KQLFunctionException(String message) {
    super(message);
  }

  public KQLFunctionException(String s, Throwable throwable) {
    super(s, throwable);
  }
}
