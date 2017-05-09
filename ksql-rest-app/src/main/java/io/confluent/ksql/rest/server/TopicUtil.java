/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicResults;
import org.apache.kafka.clients.admin.ListTopicsResults;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class TopicUtil {
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
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public void ensureTopicExists(String topic) throws InterruptedException, ExecutionException {
    log.debug("Checking for existence of topic {}", topic);
    ListTopicsResults topics = client.listTopics();
    if (!topics.names().get().contains(topic)) {
      log.info("Creating topic {}", topic);
      NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
      CreateTopicResults createTopicResults = client.createTopics(Collections.singleton(newTopic));
      createTopicResults.all().get();
    }
  }
}