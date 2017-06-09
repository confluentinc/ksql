/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import org.apache.kafka.streams.errors.StreamsException;

public class KsqlException extends StreamsException {

  public KsqlException(String message) {
    super(message);
  }

  public KsqlException(String s, Throwable throwable) {
    super(s, throwable);
  }
}
