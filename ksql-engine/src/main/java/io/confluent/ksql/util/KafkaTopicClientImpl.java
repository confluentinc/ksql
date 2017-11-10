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
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
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

  private boolean isDeleteTopicEnabled = false;

  public KafkaTopicClientImpl(final AdminClient adminClient) {
    this.adminClient = adminClient;
    init();
  }

  @Override
  public void createTopic(final String topic, final int numPartitions, final short replicatonFactor) {
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
      log.debug("Did not create topic {} with {} partitions and replication-factor {} since it already exists", topic,
              numPartitions, replicatonFactor);
      return;
    }
    NewTopic newTopic = new NewTopic(topic, numPartitions, replicatonFactor);
    try {
      log.info("Creating topic '{}'", topic);
      adminClient.createTopics(Collections.singleton(newTopic)).all().get();

    } catch (InterruptedException | ExecutionException e) {
      throw new KafkaResponseGetFailedException("Failed to guarantee existence of topic " +
                                                topic, e);
    }
  }

  @Override
  public boolean isTopicExists(final String topic) {
    log.trace("Checking for existence of topic '{}'", topic);
    return listTopicNames().contains(topic);
  }

  @Override
  public Set<String> listTopicNames() {
    try {
      return adminClient.listTopics().names().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new KafkaResponseGetFailedException("Failed to retrieve Kafka Topic names", e);
    }
  }

  @Override
  public Map<String, TopicDescription> describeTopics(final Collection<String> topicNames) {
    try {
      return adminClient.describeTopics(topicNames).all().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new KafkaResponseGetFailedException("Failed to Describe Kafka Topics", e);
    }
  }

  @Override
  public void deleteTopics(final List<String> topicsToDelete) {
    if (!isDeleteTopicEnabled) {
      log.info("Cannot delete topics since 'delete.topic.enable' is false. ");
      return;
    }
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
      throw new KsqlException("Failed to clean up topics: " + failList.stream()
          .collect(Collectors.joining(",")));
    }
  }

  @Override
  public void deleteInternalTopics(final String applicationId) {
    if (!isDeleteTopicEnabled) {
      log.warn("Cannot delete topics since 'delete.topic.enable' is false. ");
      return;
    }
    try {
      Set<String> topicNames = listTopicNames();
      List<String> internalTopics = new ArrayList<>();
      for (String topicName: topicNames) {
        if (isInternalTopic(topicName, applicationId)) {
          internalTopics.add(topicName);
        }
      }
      if (!internalTopics.isEmpty()) {
        deleteTopics(internalTopics);
      }
    } catch (Exception e) {
      log.error("Exception while trying to clean up internal topics for application id: {}.",
                applicationId, e);
    }
  }

  private void init() {
    try {
      DescribeClusterResult describeClusterResult = adminClient.describeCluster();
      List<Node> nodes = new ArrayList<>(describeClusterResult.nodes().get());
      if (!nodes.isEmpty()) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER,
                                                     String.valueOf(nodes.get(0).id()));
        DescribeConfigsResult
            describeConfigsResult = adminClient.describeConfigs(Collections.singleton(resource));
        Map<ConfigResource, Config> config = describeConfigsResult.all().get();

        this.isDeleteTopicEnabled = config.get(resource)
            .entries()
            .stream()
            .anyMatch(configEntry -> configEntry.name().equalsIgnoreCase("delete.topic.enable")
                                     && configEntry.value().equalsIgnoreCase("true"));


      } else {
        log.warn("No available broker found to fetch config info.");
        throw new KsqlException("Could not fetch broker information. KSQL cannot initialize "
                                + "AdminCLient.");
      }
    } catch (InterruptedException | ExecutionException ex) {
      log.error("Failed to initialize TopicClient: {}", ex.getMessage());
      throw new KsqlException("Could not fetch broker information. KSQL cannot initialize "
                              + "AdminCLient.");
    }
  }

  private boolean isInternalTopic(final String topicName, String applicationId) {
    return topicName.startsWith(applicationId + "-")
           && (topicName.endsWith("-changelog") || topicName.endsWith("-repartition"));
  }

  public void close() {
    this.adminClient.close();
  }

}