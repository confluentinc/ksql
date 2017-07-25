/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.exception;

public class KafkaTopicException extends KafkaTopicClientException {
  public KafkaTopicException(String message) {
    super(message);
  }

  public KafkaTopicException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
