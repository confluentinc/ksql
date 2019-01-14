/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.services;

import io.confluent.ksql.util.KafkaTopicClient;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;

/**
 * A topic client to use when trying out operations.
 *
 * <p>The client will not make changes to the remote Kafka cluster.
 */
class TryKafkaTopicClient implements KafkaTopicClient {

  private final KafkaTopicClient delegate;

  TryKafkaTopicClient(final KafkaTopicClient delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public void createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor,
      final Map<String, ?> configs
  ) {
    // Todo(ac): Might want to track so that later `isTopicExists` succeed...
  }

  @Override
  public boolean isTopicExists(final String topic) {
    return delegate.isTopicExists(topic);
  }

  @Override
  public Set<String> listTopicNames() {
    return delegate.listTopicNames();
  }

  @Override
  public Set<String> listNonInternalTopicNames() {
    return delegate.listNonInternalTopicNames();
  }

  @Override
  public Map<String, TopicDescription> describeTopics(final Collection<String> topicNames) {
    return delegate.describeTopics(topicNames);
  }

  @Override
  public Map<String, String> getTopicConfig(final String topicName) {
    return delegate.getTopicConfig(topicName);
  }

  @Override
  public boolean addTopicConfig(final String topicName, final Map<String, ?> overrides) {
    return false;
  }

  @Override
  public TopicCleanupPolicy getTopicCleanupPolicy(final String topicName) {
    return delegate.getTopicCleanupPolicy(topicName);
  }

  @Override
  public void deleteTopics(final Collection<String> topicsToDelete) {
  }

  @Override
  public void deleteInternalTopics(final String applicationId) {
  }
}
