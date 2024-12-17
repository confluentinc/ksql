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

import static io.confluent.ksql.util.LimitedProxyBuilder.methodParams;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.topic.TopicProperties;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.LimitedProxyBuilder;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclOperation;

/**
 * A topic client to use when trying out operations.
 *
 * <p>The client will not make changes to the remote Kafka cluster.
 */
@SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Methods invoked via reflection.
@SuppressWarnings("unused")  // Methods invoked via reflection.
final class SandboxedKafkaTopicClient {

  static KafkaTopicClient createProxy(final KafkaTopicClient delegate,
                                      final Supplier<Admin> sharedAdmin) {
    final SandboxedKafkaTopicClient sandbox = new SandboxedKafkaTopicClient(delegate, sharedAdmin);

    return LimitedProxyBuilder.forClass(KafkaTopicClient.class)
        .forward("createTopic",
            methodParams(String.class, int.class, short.class), sandbox)
        .forward("createTopic",
            methodParams(String.class, int.class, short.class, Map.class), sandbox)
        .forward("isTopicExists", methodParams(String.class), sandbox)
        .forward("describeTopic", methodParams(String.class), sandbox)
        .forward("getTopicConfig", methodParams(String.class), sandbox)
        .forward("describeTopic", methodParams(String.class, Boolean.class), sandbox)
        .forward("describeTopics", methodParams(Collection.class), sandbox)
        .forward("describeTopics", methodParams(Collection.class, Boolean.class), sandbox)
        .forward("deleteTopics", methodParams(Collection.class), sandbox)
        .forward("listTopicsStartOffsets", methodParams(Collection.class), sandbox)
        .forward("listTopicsEndOffsets", methodParams(Collection.class), sandbox)
        .build();
  }

  private static final String DEFAULT_REPLICATION_PROP = "default.replication.factor";

  private final KafkaTopicClient delegate;
  private final Supplier<Admin> adminClient;

  private final Map<String, TopicDescription> createdTopics = new HashMap<>();
  private final Map<String, Map<String, String>> createdTopicsConfig = new HashMap<>();

  private SandboxedKafkaTopicClient(final KafkaTopicClient delegate,
                                    final Supplier<Admin> sharedAdminClient) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.adminClient = Objects.requireNonNull(sharedAdminClient, "sharedAdminClient");
  }

  private boolean createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor
  ) {
    return createTopic(topic, numPartitions, replicationFactor, Collections.emptyMap());
  }

  private boolean createTopic(
      final String topic,
      final int numPartitions,
      final short replicationFactor,
      final Map<String, Object> configs
  ) {
    if (isTopicExists(topic)) {
      final Optional<Long> retentionMs = KafkaTopicClient.getRetentionMs(configs);
      validateTopicProperties(topic, numPartitions, replicationFactor, retentionMs);
      return false;
    }

    final short resolvedReplicationFactor = replicationFactor == TopicProperties.DEFAULT_REPLICAS
        ? getDefaultClusterReplication()
        : replicationFactor;

    final List<Node> replicas = IntStream.range(0, resolvedReplicationFactor)
        .mapToObj(idx -> (Node) null)
        .collect(Collectors.toList());

    final List<TopicPartitionInfo> partitions = IntStream.range(1, numPartitions + 1)
        .mapToObj(partition -> new TopicPartitionInfo(
            partition,
            null,
            replicas,
            Collections.emptyList()))
        .collect(Collectors.toList());

    // This is useful to validate permissions to create the topic
    delegate.validateCreateTopic(topic, numPartitions, resolvedReplicationFactor, configs);

    createdTopics.put(topic, new TopicDescription(
        topic,
        false,
        partitions,
        Sets.newHashSet(AclOperation.READ, AclOperation.WRITE)
    ));

    createdTopicsConfig.put(topic, toStringConfigs(configs));
    return true;
  }

  private short getDefaultClusterReplication() {
    try {
      final String defaultReplication = KafkaClusterUtil.getConfig(adminClient.get())
          .get(DEFAULT_REPLICATION_PROP)
          .value();
      return Short.parseShort(defaultReplication);
    } catch (final KsqlServerException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlServerException("Could not get default replication from Kafka cluster!", e);
    }
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

  public TopicDescription describeTopic(final String topicName,
                                        final Boolean skipRetriesOnFailure) {
    return describeTopics(ImmutableList.of(topicName), skipRetriesOnFailure).get(topicName);
  }

  private Map<String, TopicDescription> describeTopics(final Collection<String> topicNames) {
    return describeTopics(topicNames, false);
  }

  private Map<String, TopicDescription> describeTopics(final Collection<String> topicNames,
                                                       final Boolean skipRetriesOnFailure) {
    final Map<String, TopicDescription> descriptions = topicNames.stream()
        .map(createdTopics::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(TopicDescription::name, Function.identity()));

    final Set<String> topicsToFetch = new HashSet<>(topicNames);
    topicsToFetch.removeAll(descriptions.keySet());
    if (topicsToFetch.isEmpty()) {
      return descriptions;
    }

    final Map<String, TopicDescription> remainingTopicDescriptionMap =
                delegate.describeTopics(topicsToFetch, skipRetriesOnFailure);

    descriptions.putAll(remainingTopicDescriptionMap);
    return descriptions;
  }

  public Map<String, String> getTopicConfig(final String topicName) {
    if (createdTopicsConfig.containsKey(topicName)) {
      return createdTopicsConfig.get(topicName);
    }
    return delegate.getTopicConfig(topicName);
  }

  private void deleteTopics(final Collection<String> topicsToDelete) {
    topicsToDelete.forEach(createdTopics::remove);
    delegate.deleteTopics(topicsToDelete);
  }

  private void validateTopicProperties(
      final String topic,
      final int requiredNumPartition,
      final int requiredNumReplicas,
      final Optional<Long> requiredRetentionMs
  ) {
    final TopicDescription existingTopic = describeTopic(topic);
    final Map<String, String> existingConfig = getTopicConfig(topic);
    TopicValidationUtil
        .validateTopicProperties(
            requiredNumPartition,
            requiredNumReplicas,
            requiredRetentionMs,
            existingTopic,
            existingConfig);
  }

  private Map<TopicPartition, Long> listTopicsStartOffsets(final Collection<String> topics) {
    return delegate.listTopicsStartOffsets(topics);
  }

  private Map<TopicPartition, Long> listTopicsEndOffsets(final Collection<String> topics) {
    return delegate.listTopicsEndOffsets(topics);
  }

  private static Map<String, String> toStringConfigs(final Map<String, ?> configs) {
    return configs.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
  }
}
