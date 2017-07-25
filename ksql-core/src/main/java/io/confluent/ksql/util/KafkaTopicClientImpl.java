/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KafkaTopicException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaTopicClientImpl implements KafkaTopicClient {
  private static final Logger log = LoggerFactory.getLogger(KafkaTopicClient.class);

  private final AdminClient client;

  public KafkaTopicClientImpl(KsqlConfig ksqlConfig) {
    this.client = AdminClient.create(ksqlConfig.getKsqlConfigProps());
  }

  public void createTopic(String topic, int numPartitions, short replicatonFactor) {
    log.info("Creating topic '{}'", topic);
    if (isTopicExists(topic)) {
      Map<String, TopicDescription> topicDescriptions = describeTopics(Arrays.asList(topic));
      TopicDescription topicDescription = topicDescriptions.get(topic);
      if (topicDescription.partitions().size() != numPartitions ||
          topicDescription.partitions().get(0).replicas().size() != replicatonFactor) {
        throw new KafkaTopicException(String.format("Topic '%s' does not conform to the "
                                                    + "requirements.", topic));
      }
      // Topic with the partitons and replicas exists, reuse it!
      return;
    }
    NewTopic newTopic = new NewTopic(topic, numPartitions, replicatonFactor);
    try {
      client.createTopics(Collections.singleton(newTopic)).all().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new KafkaResponseGetFailedException("Failed to guarantee existence of topic " + topic, e);
    }
  }

  public boolean isTopicExists(String topic) {
    log.debug("Checking for existence of topic '{}'", topic);
    return listTopicNames().contains(topic);
  }

  public Set<String> listTopicNames() {
    try {
      return client.listTopics().names().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new KafkaResponseGetFailedException("Failed to retrieve kafka topic names", e);
    }
  }

  public Map<String, TopicDescription> describeTopics(Collection<String> topicNames) {
    try {
      return client.describeTopics(topicNames).all().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new KafkaResponseGetFailedException("Failed to describe kafka topics", e);
    }
  }

  public void close() {
    client.close();
  }

}