/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  /**
   * Synchronously check for the existence of a topic and, in the event that it does not exist,
   * create it with a single
   * partition and a replication factor of 1.
   * TODO: Think about num partitions / replication factor to use here
   * @param topic The name of the topic to create
   * @return Whether or not the operation succeeded.
   */
  public boolean ensureTopicExists(String topic, int numPartitions, short replicatonFactor) {
    try {
      if (!topicExists(topic)) {
        log.info("Creating topic '{}'", topic);
        NewTopic newTopic = new NewTopic(topic, numPartitions, replicatonFactor);
        CreateTopicsResult createTopicResults = client
            .createTopics(Collections.singleton(newTopic));
        createTopicResults.all().get();
      }
      return true;
    } catch (Exception exception) {
      log.warn("Exception encountered while ensuring topic '{}' exists", topic, exception);
      return false;
    }
  }

  public boolean createTopic(String topic, int numPartitions, short replicatonFactor) {
    try {
      log.info("Creating topic '{}'", topic);
      NewTopic newTopic = new NewTopic(topic, numPartitions, replicatonFactor);
      CreateTopicsResult createTopicResults = client.createTopics(Collections.singleton(newTopic));
      createTopicResults.all().get();
      return true;
    } catch (Exception exception) {
      log.warn("Exception encountered while ensuring topic '{}' exists", topic, exception);
      return false;
    }
  }

  /**
   * Synchronously check for the existence of a topic.
   * @param topic The name of the topic to check for
   * @return Whether or not the topic already exists
   */
  private boolean topicExists(String topic) throws InterruptedException, ExecutionException {
    log.debug("Checking for existence of topic '{}'", topic);
    return listTopicNames().contains(topic);
  }

  /**
   * TODO: This is a synchronous call, optimize if necessary
   * @return
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public Set<String> listTopicNames() throws ExecutionException, InterruptedException {
    return client.listTopics().names().get();
  }

  /**
   * Close the underlying Kafka admin client.
   */
  public void close() {
    client.close();
  }
}
