/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function;

import io.confluent.ksql.util.KsqlException;

public class KsqlFunctionException extends KsqlException {

  public KsqlFunctionException(String message) {
    super(message);
  }

  public KsqlFunctionException(String s, Throwable throwable) {
    super(s, throwable);
  }
}
