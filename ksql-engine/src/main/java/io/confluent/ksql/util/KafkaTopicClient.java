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

import org.apache.kafka.clients.admin.TopicDescription;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface KafkaTopicClient extends Closeable {

  enum TopicCleanupPolicy {
    COMPACT,
    DELETE,
    COMPACT_DELETE
  }

  /**
   * Create a new topic with the specified name, numPartitions and replicationFactor.
   * [warn] synchronous call to get the response
   *
   * @param topic name of the topic to create
   */
  void createTopic(String topic, int numPartitions, short replicationFactor);

  /**
   * Create a new topic with the specified name, numPartitions and replicationFactor.
   * [warn] synchronous call to get the response
   * @param topic   name of the topic to create
   * @param configs any additional topic configs to use
   */
  void createTopic(
      String topic,
      int numPartitions,
      short replicationFactor,
      Map<String, ?> configs
  );

  /**
   * [warn] synchronous call to get the response
   *
   * @param topic name of the topic
   * @return whether the topic exists or not
   */
  boolean isTopicExists(String topic);

  /**
   * [warn] synchronous call to get the response
   *
   * @return set of existing topic names
   */
  Set<String> listTopicNames();

  /**
   * Synchronous call to retrieve list of internal topics
   *
   * @return set of all non-internal topics
   */
  Set<String> listNonInternalTopicNames();

  /**
   * Synchronous call to get a one or more topic's description.
   *
   * @param topicNames topicNames to describe
   * @return map of topic name to description.
   */
  Map<String, TopicDescription> describeTopics(Collection<String> topicNames);

  /**
   * Synchronous call to get the config of a topic.
   *
   * @param topicName the name of the topic.
   * @return map of topic config if the topic is known, {@link Optional#empty()} otherwise.
   */
  Map<String, String> getTopicConfig(String topicName);

  /**
   * Synchronous call to write topic config overrides to ZK.
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
   * Synchronous call to get a topic's cleanup policy
   *
   * @param topicName topicNames to retrieve cleanup policy for.
   * @return the clean up policy of the topic.
   */
  TopicCleanupPolicy getTopicCleanupPolicy(String topicName);

  /**
   * Delete the list of the topics in the given list.
   */
  void deleteTopics(List<String> topicsToDelete);

  /**
   * Delete the internal topics of a given application.
   */
  void deleteInternalTopics(String applicationId);

  /**
   * Close the underlying Kafka admin client.
   */
  void close();
}
