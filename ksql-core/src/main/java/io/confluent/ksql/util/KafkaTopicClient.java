/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KafkaTopicException;
import org.apache.kafka.clients.admin.TopicDescription;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface KafkaTopicClient extends Closeable {

  /**
   * Create a new topic with the specified name, numPartitions and replicatonFactor.
   * [warn] synchronous call to get the response
   * @param topic name of the topic to create
   * @param numPartitions
   * @param replicatonFactor
   * @throws KafkaTopicException
   * @throws KafkaResponseGetFailedException
   */
  void createTopic(String topic, int numPartitions, short replicatonFactor);

  /**
   * [warn] synchronous call to get the response
   * @param topic name of the topic
   * @return whether the topic exists or not
   * @throws KafkaResponseGetFailedException
   */
  boolean isTopicExists(String topic);

  /**
   * [warn] synchronous call to get the response
   * @return set of existing topic names
   * @throws KafkaResponseGetFailedException
   */
  Set<String> listTopicNames();

  /**
   * [warn] synchronous call to get the response
   * @param topicNames topicNames to describe
   * @throws KafkaResponseGetFailedException
   */
  Map<String, TopicDescription> describeTopics(Collection<String> topicNames);

  /**
   * Close the underlying Kafka admin client.
   */
  void close();

}
