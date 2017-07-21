/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.exception;

public class KafkaTopicExistsException extends KafkaTopicClientException {
  public KafkaTopicExistsException(String message) {
    super(message);
  }

  public KafkaTopicExistsException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
