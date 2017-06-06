/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.server;

import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicConfig;
import org.apache.kafka.streams.processor.internals.StreamsKafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import io.confluent.ksql.util.KsqlException;

public class TopicUtil implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(TopicUtil.class);

  private final StreamsKafkaClient streamsKafkaClient;

  public TopicUtil(KsqlRestConfig config) {
    this.streamsKafkaClient =
        new StreamsKafkaClient(new StreamsConfig(config.getKsqlStreamsProperties()));
  }

  /**
   * Synchronously check for the existence of a topic and, in the event that it does not exist,
   * create it with a single partition and a replication factor of 1.
   * TODO: Think about num partitions / replication factor to use here
   * @param topic The name of the topic to create
   * @return Whether or not the operation succeeded.
   */
  public boolean ensureTopicExists(String topic) {
    try {
      if (!topicExists(topic)) {
        log.info("Creating topic '{}'", topic);
        InternalTopicConfig internalTopicConfig = new InternalTopicConfig(
            topic,
            Utils.mkSet(
                InternalTopicConfig.CleanupPolicy.compact,
                InternalTopicConfig.CleanupPolicy.delete),
            Collections.emptyMap()
        );
        Map<InternalTopicConfig, Integer> topics = new HashMap<>();
        topics.put(internalTopicConfig, 1);
        streamsKafkaClient.createTopics(topics, 1, 1000000L, streamsKafkaClient.fetchMetadata());
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
    final MetadataResponse metadata = streamsKafkaClient.fetchMetadata();
    final Collection<MetadataResponse.TopicMetadata> topicsMetadata = metadata.topicMetadata();
    for (MetadataResponse.TopicMetadata topicMetadata: topicsMetadata) {
      if (topicMetadata.topic().equalsIgnoreCase(topic)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Close the underlying streams Kafka client.
   */
  @Override
  public void close() {
    try {
      streamsKafkaClient.close();
    } catch (IOException e) {
      throw new KsqlException("Exception encountered while closing StreamsKafkaClient.", e);
    }
  }
}
