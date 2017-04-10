/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest;

import io.confluent.adminclient.KafkaAdminClient;

import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class TopicUtil {
  private static final Logger log = LoggerFactory.getLogger(TopicUtil.class);

  private final KafkaAdminClient client;


  public TopicUtil(KafkaAdminClient client) {
    this.client = client;
  }

  public void ensureTopicExists(String topic) {
    log.info(String.format("Checking for existence of topic %s", topic));
    if (client.topics(Collections.singletonList(topic)).isEmpty()) {
      log.info(String.format("Creating topic %s", topic));
      // TODO: Think about num partitions / replication factor to use here
      CreateTopicsRequest.TopicDetails topicDetails =
          new CreateTopicsRequest.TopicDetails(1, (short) 1);
      client.createTopics(Collections.singletonMap(topic, topicDetails));
    }
  }
}