/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.exception;

import io.confluent.ksql.util.KsqlException;

public class ParseFailedException extends KsqlException {
  public ParseFailedException(String message) {
    super(message);
  }

  public ParseFailedException(String message, Throwable throwable) {
    super(message, throwable);
  }
}