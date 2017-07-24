/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.exception;

public abstract class KafkaTopicClientException extends RuntimeException {
  public KafkaTopicClientException(String message) {
    super(message);
  }

  public KafkaTopicClientException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
