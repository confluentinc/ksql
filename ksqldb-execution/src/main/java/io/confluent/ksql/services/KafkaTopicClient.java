/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.services;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.exception.KafkaTopicExistsException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;

/**
 * Note: all methods are synchronous, i.e. they wait for responses from Kafka before returning.
 */
public interface KafkaTopicClient {

  enum TopicCleanupPolicy {
    COMPACT,
    DELETE,
    COMPACT_DELETE
  }

  default boolean validateCreateTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor
  ) {
    return validateCreateTopic(topic, numPartitions, replicationFactor, Collections.emptyMap());
  }

  default boolean validateCreateTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor,
      final Map<String, ?> configs
  ) {
    return createTopic(
        topic,
        numPartitions,
        replicationFactor,
        configs,
        new CreateTopicsOptions().validateOnly(true)
    );
  }

  /**
   * Create a new topic with the specified name, numPartitions and replicationFactor.
   *
   * <p>If the topic already exists the method checks that partition count <i>matches</i>matches
   * {@code numPartitions} and that the replication factor is <i>at least</i> {@code
   * replicationFactor}
   *
   * @param topic             name of the topic to create
   * @param replicationFactor the rf of the topic.
   * @param numPartitions     the partition count of the topic.
   *
   * @return true if the topic was created as a result of this call
   */
  default boolean createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor
  ) {
    return createTopic(topic, numPartitions, replicationFactor, Collections.emptyMap());
  }

  /**
   * Create a new topic with the specified name, numPartitions and replicationFactor.
   *
   * <p>If the topic already exists the method checks that partition count <i>matches</i>matches
   * {@code numPartitions} and that the replication factor is <i>at least</i> {@code
   * replicationFactor}
   *
   * @param topic             name of the topic to create
   * @param replicationFactor the replication factor for the new topic, or to use the default
   *                          replication of the cluster
   * @param numPartitions     the partition count of the topic.
   * @param configs           any additional topic configs to use
   */
  default boolean createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor,
      final Map<String, ?> configs
  ) {
    return createTopic(
        topic,
        numPartitions,
        replicationFactor,
        configs,
        new CreateTopicsOptions()
    );
  }

  /**
   * Create a new topic with the specified name, numPartitions and replicationFactor.
   *
   * <p>If the topic already exists the method checks that partition count <i>matches</i>matches
   * {@code numPartitions} and that the replication factor is <i>at least</i> {@code
   * replicationFactor}
   *
   * @param topic             name of the topic to create
   * @param replicationFactor the replication factor for the new topic, or {@link
   *                          io.confluent.ksql.topic.TopicProperties#DEFAULT_REPLICAS} to use the
   *                          default replication of the cluster
   * @param numPartitions     the partition count of the topic.
   * @param configs           any additional topic configs to use
   * @param createOptions     the options to use when creating the new topic
   *
   * @return true if the topic was created as a result of this call
   */
  boolean createTopic(
      String topic,
      int numPartitions,
      short replicationFactor,
      Map<String, ?> configs,
      CreateTopicsOptions createOptions
  );

  /**
   * @param topic name of the topic
   * @return whether the topic exists or not
   */
  boolean isTopicExists(String topic);

  /**
   * @return set of existing topic names
   */
  Set<String> listTopicNames();

  /**
   * Call to get a one or more topic's description.
   *
   * @param topicNames topicNames to describe
   * @return map of topic name to description.
   * @throws KafkaTopicExistsException if any of the topic do not exist.
   */
  Map<String, TopicDescription> describeTopics(Collection<String> topicNames);

  /**
   * Call to get a one topic's description.
   *
   * @param topicName topicName to describe
   * @return the description if the topic
   * @throws KafkaTopicExistsException if the topic does not exist.
   */
  default TopicDescription describeTopic(final String topicName) {
    return describeTopics(ImmutableList.of(topicName)).get(topicName);
  }

  /**
   * Call to get the config of a topic.
   *
   * @param topicName the name of the topic.
   * @return map of topic config if the topic is known, {@link Optional#empty()} otherwise.
   */
  Map<String, String> getTopicConfig(String topicName);

  /**
   * Call to write topic config overrides to ZK.
   *
   * <p>This will add additional overrides, and not replace any existing, unless they have the same
   * name.
   *
   * <p>Note: each broker will pick up this change asynchronously.
   *
   * @param topicName the name of the topic.
   * @param overrides new config overrides to add.
   * @return {@code true} if any of the {@code overrides} did not already exist
   */
  boolean addTopicConfig(String topicName, Map<String, ?> overrides);

  /**
   * Call to get a topic's cleanup policy.
   *
   * @param topicName topicNames to retrieve cleanup policy for.
   * @return the clean up policy of the topic.
   */
  TopicCleanupPolicy getTopicCleanupPolicy(String topicName);

  /**
   * Delete the topics in the given collection.
   */
  void deleteTopics(Collection<String> topicsToDelete);

  /**
   * Delete the internal topics of a given application.
   */
  void deleteInternalTopics(String applicationId);

  Map<TopicPartition, Long> listTopicsOffsets(
      Collection<String> topicNames,
      OffsetSpec offsetSpec
  );

  default Map<TopicPartition, Long> listTopicsStartOffsets(Collection<String> topicNames) {
    return listTopicsOffsets(topicNames, OffsetSpec.earliest());
  }

  default Map<TopicPartition, Long> listTopicsEndOffsets(Collection<String> topicName) {
    return listTopicsOffsets(topicName, OffsetSpec.latest());
  }

  static Optional<Long> getRetentionMs(Map<String, ?> config) {
    if (config.containsKey(TopicConfig.RETENTION_MS_CONFIG)) {
      return Optional.ofNullable(
          Long.parseLong(String.valueOf(config.get(TopicConfig.RETENTION_MS_CONFIG))));
    } else {
      return Optional.empty();
    }
  }

}
