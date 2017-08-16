/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.server.mock;

import io.confluent.ksql.util.KafkaTopicClient;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Fake Kafka Client is for test only, none of its methods should be called.
 */
public class MockKafkaTopicClient implements KafkaTopicClient {

  @Override
  public void createTopic(String topic, int numPartitions, short replicatonFactor) {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public boolean isTopicExists(String topic) {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public Set<String> listTopicNames() {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public Map<String, TopicDescription> describeTopics(Collection<String> topicNames) {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

}