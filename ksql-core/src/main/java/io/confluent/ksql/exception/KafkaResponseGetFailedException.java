/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.exception;

public class KafkaResponseGetFailedException extends KafkaTopicClientException {
  public KafkaResponseGetFailedException(String message) {
    super(message);
  }

  public KafkaResponseGetFailedException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
