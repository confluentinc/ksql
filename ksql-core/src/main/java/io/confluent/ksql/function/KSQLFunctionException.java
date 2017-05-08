/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function;

import io.confluent.ksql.util.KSQLException;

public class KSQLFunctionException extends KSQLException {

  public KSQLFunctionException(String message) {
    super(message);
  }

  public KSQLFunctionException(String s, Throwable throwable) {
    super(s, throwable);
  }
}
