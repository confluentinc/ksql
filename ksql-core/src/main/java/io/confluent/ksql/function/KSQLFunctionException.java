/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function;

import io.confluent.ksql.util.KQLException;

public class KQLFunctionException extends KQLException {

  public KQLFunctionException(String message) {
    super(message);
  }

  public KQLFunctionException(String s, Throwable throwable) {
    super(s, throwable);
  }
}
