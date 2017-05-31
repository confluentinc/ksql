/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class TopicUtil implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(TopicUtil.class);

  private final AdminClient client;

  public TopicUtil(AdminClient client) {
    this.client = client;
  }

  /**
   * Synchronously check for the existence of a topic and, in the event that it does not exist, create it with a single
   * partition and a replication factor of 1.
   * TODO: Think about num partitions / replication factor to use here
   * @param topic The name of the topic to create
   * @return Whether or not the operation succeeded.
   */
  public boolean ensureTopicExists(String topic) {
    try {
      if (!topicExists(topic)) {
        log.info("Creating topic '{}'", topic);
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
        CreateTopicsResult createTopicResults = client.createTopics(Collections.singleton(newTopic));
        createTopicResults.all().get();
      }
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
  public boolean topicExists(String topic) throws InterruptedException, ExecutionException {
    log.debug("Checking for existence of topic '{}'", topic);
    ListTopicsResult topics = client.listTopics();
    return topics.names().get().contains(topic);
  }

  /**
   * Close the underlying Kafka admin client.
   */
  @Override
  public void close() {
    client.close();
  }
}
