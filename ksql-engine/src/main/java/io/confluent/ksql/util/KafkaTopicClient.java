/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;

public interface KafkaTopicClient {

  enum TopicCleanupPolicy {
    COMPACT,
    DELETE,
    COMPACT_DELETE
  }

  /**
   * Create a new topic with the specified name, numPartitions and replicationFactor.
   *
   * <p>If the topic already exists the method checks that partition count <i>matches</i>matches
   * {@code numPartitions} and that the replication factor is <i>at least</i>
   * {@code replicationFactor}
   *
   * <p>[warn] synchronous call to get the response
   *
   * @param topic name of the topic to create
   * @param replicationFactor the rf of the topic.
   * @param numPartitions the partition count of the topic.
   */
  default void createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor) {
    createTopic(topic, numPartitions, replicationFactor, Collections.emptyMap());
  }

  /**
   * Create a new topic with the specified name, numPartitions and replicationFactor.
   *
   * <p>If the topic already exists the method checks that partition count <i>matches</i>matches
   * {@code numPartitions} and that the replication factor is <i>at least</i>
   * {@code replicationFactor}
   *
   * <p>[warn] synchronous call to get the response
   *
   * @param topic name of the topic to create
   * @param replicationFactor the rf of the topic.
   * @param numPartitions the partition count of the topic.
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
   * Synchronous call to get a one topic's description.
   *
   * @param topicName topicName to describe
   * @return the description if the topic exists, or else {@code Optional.empty()}
   */
  default TopicDescription describeTopic(String topicName) {
    return describeTopics(ImmutableList.of(topicName)).get(topicName);
  }

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
   * Delete the topics in the given collection.
   */
  void deleteTopics(Collection<String> topicsToDelete);

  /**
   * Delete the internal topics of a given application.
   */
  void deleteInternalTopics(String applicationId);
}
