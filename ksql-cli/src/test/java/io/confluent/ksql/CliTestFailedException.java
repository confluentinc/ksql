/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql;

public class CliTestFailedException extends RuntimeException {

  public CliTestFailedException(Throwable cause) {
    super(cause);
  }

}
