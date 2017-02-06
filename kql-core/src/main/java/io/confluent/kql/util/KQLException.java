/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.kql.util;

import org.apache.kafka.streams.errors.StreamsException;

public class KQLException extends StreamsException {

  public KQLException(String message) {
    super(message);
  }

  public KQLException(String s, Throwable throwable) {
    super(s, throwable);
  }
}
