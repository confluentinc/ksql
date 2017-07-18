/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Fake Kafka Client is for test only, none of its methods should be called.
 */
public class FakeKafkaTopicClient implements KafkaTopicClient {
  @Override
  public boolean ensureTopicExists(String topic, int numPartitions, short replicatonFactor) {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public boolean createTopic(String topic, int numPartitions, short replicatonFactor) {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public Set<String> listTopicNames() throws ExecutionException, InterruptedException {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }
}
