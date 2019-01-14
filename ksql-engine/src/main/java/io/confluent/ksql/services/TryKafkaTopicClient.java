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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

/**
 * A topic client to use when trying out operations.
 *
 * <p>The client will not make changes to the remote Kafka cluster.
 */
class TryKafkaTopicClient implements KafkaTopicClient {

  // Todo(ac): tidy up & test.

  private final KafkaTopicClient delegate;

  private final Map<String, TopicDescription> createdTopics = new HashMap<>();

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
    final List<Node> replicas = IntStream.range(0, replicationFactor)
        .mapToObj(idx -> (Node) null)
        .collect(Collectors.toList());

    final List<TopicPartitionInfo> partitions = IntStream.range(1, numPartitions + 1)
        .mapToObj(partition -> new TopicPartitionInfo(
            partition,
            null,
            replicas,
            Collections.emptyList()))
        .collect(Collectors.toList());

    createdTopics.put(topic, new TopicDescription(topic, false, partitions));
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
    final Map<String, TopicDescription> descriptions = topicNames.stream()
        .map(createdTopics::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(TopicDescription::name, Function.identity()));

    final HashSet<String> remaining = new HashSet<>(topicNames);
    remaining.removeAll(descriptions.keySet());
    if (remaining.isEmpty()) {
      return descriptions;
    }

    final Map<String, TopicDescription> fromKafka = delegate.describeTopics(remaining);

    descriptions.putAll(fromKafka);
    return descriptions;
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
