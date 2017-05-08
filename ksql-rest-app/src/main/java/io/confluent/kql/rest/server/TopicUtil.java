/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest.server;

import io.confluent.adminclient.KafkaAdminClient;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

// TODO: Update to official AdminClient once it's available

public class TopicUtil {
  private static final Logger log = LoggerFactory.getLogger(TopicUtil.class);

  private final KafkaAdminClient client;

  public TopicUtil(KafkaAdminClient client) {
    this.client = client;
  }

  public void ensureTopicExists(String topic) {
    log.debug("Checking for existence of topic {}", topic);
    if (client.topics(Collections.singletonList(topic)).isEmpty()) {
      log.info("Creating topic {}", topic);
      // TODO: Think about num partitions / replication factor to use here
      CreateTopicsRequest.TopicDetails topicDetails =
          new CreateTopicsRequest.TopicDetails(1, (short) 1);
      client.createTopics(Collections.singletonMap(topic, topicDetails));
    }
  }
}