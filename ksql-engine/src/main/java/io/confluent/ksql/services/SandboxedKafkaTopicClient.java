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

import static io.confluent.ksql.services.SandboxProxyBuilder.methodParams;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
@SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Methods invoked via reflection.
@SuppressWarnings("unused")  // Methods invoked via reflection.
final class SandboxedKafkaTopicClient {

  static KafkaTopicClient createProxy(final KafkaTopicClient delegate) {
    final SandboxedKafkaTopicClient sandbox = new SandboxedKafkaTopicClient(delegate);

    return SandboxProxyBuilder.forClass(KafkaTopicClient.class)
        .forward("createTopic",
            methodParams(String.class, int.class, short.class), sandbox)
        .forward("createTopic",
            methodParams(String.class, int.class, short.class, Map.class), sandbox)
        .forward("isTopicExists", methodParams(String.class), sandbox)
        .forward("describeTopic", methodParams(String.class), sandbox)
        .forward("describeTopics", methodParams(Collection.class), sandbox)
        .forward("deleteTopics", methodParams(Collection.class), sandbox)
        .build();
  }

  private final KafkaTopicClient delegate;

  private final Map<String, TopicDescription> createdTopics = new HashMap<>();

  private SandboxedKafkaTopicClient(final KafkaTopicClient delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  private void createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor
  ) {
    createTopic(topic, numPartitions, replicationFactor, Collections.emptyMap());
  }

  private void createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor,
      final Map<String, Object> configs
  ) {
    if (isTopicExists(topic)) {
      validateTopicProperties(topic, numPartitions, replicationFactor);
      return;
    }

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

  private boolean isTopicExists(final String topic) {
    if (createdTopics.containsKey(topic)) {
      return true;
    }

    return delegate.isTopicExists(topic);
  }

  public TopicDescription describeTopic(final String topicName) {
    return describeTopics(ImmutableList.of(topicName)).get(topicName);
  }

  private Map<String, TopicDescription> describeTopics(final Collection<String> topicNames) {
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

  private void deleteTopics(final Collection<String> topicsToDelete) {
    topicsToDelete.forEach(createdTopics::remove);
  }

  private void validateTopicProperties(
      final String topic,
      final int requiredNumPartition,
      final int requiredNumReplicas
  ) {
    final TopicDescription existingTopic = describeTopic(topic);
    TopicValidationUtil
        .validateTopicProperties(requiredNumPartition, requiredNumReplicas, existingTopic);
  }
}
