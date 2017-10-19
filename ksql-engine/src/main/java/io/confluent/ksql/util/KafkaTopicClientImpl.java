/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KafkaTopicException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaTopicClientImpl implements KafkaTopicClient {
  private static final Logger log = LoggerFactory.getLogger(KafkaTopicClient.class);
  private final AdminClient adminClient;

  public KafkaTopicClientImpl(final AdminClient adminClient) {
    this.adminClient = adminClient;
  }

  public void createTopic(String topic, int numPartitions, short replicatonFactor) {
    log.info("Creating topic '{}'", topic);
    if (isTopicExists(topic)) {
      Map<String, TopicDescription> topicDescriptions = describeTopics(Arrays.asList(topic));
      TopicDescription topicDescription = topicDescriptions.get(topic);
      if (topicDescription.partitions().size() != numPartitions ||
          topicDescription.partitions().get(0).replicas().size() != replicatonFactor) {
        throw new KafkaTopicException(String.format(
            "Topic '%s' does not conform to the requirements Partitions:%d v %d. Replication: %d v %d", topic,
                topicDescription.partitions().size(), numPartitions,
                topicDescription.partitions().get(0).replicas().size(), replicatonFactor
        ));
      }
      // Topic with the partitons and replicas exists, reuse it!
      return;
    }
    NewTopic newTopic = new NewTopic(topic, numPartitions, replicatonFactor);
    try {
      adminClient.createTopics(Collections.singleton(newTopic)).all().get();

    } catch (InterruptedException | ExecutionException e) {
      throw new KafkaResponseGetFailedException("Failed to guarantee existence of topic " +
                                                topic, e);
    }
  }

  public boolean isTopicExists(String topic) {
    log.debug("Checking for existence of topic '{}'", topic);
    return listTopicNames().contains(topic);
  }

  public Set<String> listTopicNames() {
    try {
      return adminClient.listTopics().names().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new KafkaResponseGetFailedException("Failed to retrieve Kafka Topic names", e);
    }
  }

  public Map<String, TopicDescription> describeTopics(Collection<String> topicNames) {
    try {
      return adminClient.describeTopics(topicNames).all()
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new KafkaResponseGetFailedException("Failed to Describe Kafka Topics", e);
    }
  }

  public void deleteTopics(List<String> topicsToDelete) {
    boolean hasDeleteErrors = false;
    final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicsToDelete);
    final Map<String, KafkaFuture<Void>> results = deleteTopicsResult.values();
    List<String> failList = new ArrayList<>();

    for (final Map.Entry<String, KafkaFuture<Void>> entry : results.entrySet()) {
      try {
        entry.getValue().get(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        failList.add(entry.getKey());
      }
    }
    if (!failList.isEmpty()) {
      throw new KsqlException("Failed to clean up topics: " + failList.stream().collect(Collectors
                                                                                          .joining(",")));
    }
  }

  public void close() {
  }

}