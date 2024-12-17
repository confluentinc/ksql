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

package io.confluent.ksql.test.util;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
 * default.
 *
 * <p>Requires a running ZooKeeper instance to connect to.  By default, it expects a ZooKeeper
 * instance running at `127.0.0.1:2181`.  You can specify a different ZooKeeper instance by setting
 * the `zookeeper.connect` parameter in the broker's configuration.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
class KafkaEmbedded {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(KafkaEmbedded.class);

  private final Properties config;
  private final KafkaServer kafka;

  /**
   * Creates and starts an embedded Kafka broker.
   *
   * @param config Broker configuration settings.  Used to modify, for example, the listeners
   *               the broker should use.  Note that you cannot change some settings such as
   *               `log.dirs`.
   */
  KafkaEmbedded(final Properties config) {
    this.config = Objects.requireNonNull(config, "config");

    final KafkaConfig kafkaConfig = new KafkaConfig(this.config, true);
    log.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
        logDir(), zookeeperConnect());

    kafka = TestUtils.createServer(kafkaConfig, new SystemTime());
    log.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
  }

  /**
   * This broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
   *
   * <p>You can use this to tell Kafka producers and consumers how to connect to this instance.
   *
   * <p>This version returns the port of the first listener.
   * @return the broker list
   */
  String brokerList() {
    final EndPoint endPoint = kafka.advertisedListeners().head();
    final String hostname = endPoint.host() == null ? "" : endPoint.host();
    return hostname + ":" + endPoint.port();
  }

  /**
   * The broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
   *
   * <p>You can use this to tell Kafka producers and consumers how to connect to this instance.
   *
   * @param securityProtocol the security protocol the returned broker list should use.
   * @return the broker list
   */
  String brokerList(final SecurityProtocol securityProtocol) {
    final Iterator<EndPoint> it = kafka.advertisedListeners().iterator();
    while (it.hasNext()) {
      final EndPoint endPoint = it.next();
      if (endPoint.securityProtocol() == securityProtocol) {
        final String hostname = endPoint.host() == null ? "" : endPoint.host();
        return hostname + ":" + endPoint.port();
      }
    }

    throw new RuntimeException("No listener with protocol " + securityProtocol);
  }

  /**
   * Stop the broker.
   */
  void stop() {
    log.debug("Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
    kafka.shutdown();
    kafka.awaitShutdown();
    log.debug("Deleting logs.dir at {} ...", logDir());
    try {
      Files.delete(Paths.get(logDir()));
    } catch (final IOException e) {
      log.error("Failed to delete log dir {}", logDir(), e);
    }
    log.debug("Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
  }

  /**
   * Create a Kafka topic with 1 partition and a replication factor of 1.
   *
   * @param topic The name of the topic.
   */
  void createTopic(final String topic) {
    createTopic(topic, 1, 1);
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (the partitions of) this topic.
   */
  void createTopic(final String topic, final int partitions, final int replication) {
    createTopic(topic, partitions, replication, ImmutableMap.of());
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  void createTopic(final String topic,
      final int partitions,
      final int replication,
      final Map<String, String> topicConfig
  ) {
    log.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
        topic, partitions, replication, topicConfig);

    try (Admin adminClient = adminClient()) {

      final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
      newTopic.configs(topicConfig);

      try {
        adminClient.createTopics(ImmutableList.of(newTopic)).all().get();
      } catch (final Exception e) {
        throw new RuntimeException("Failed to create topic:" + topic, e);
      }
    }
  }

  Set<String> getTopics() {
    try (Admin adminClient = adminClient()) {
      return getTopicNames(adminClient);
    }
  }

  /**
   * Delete topics from the cluster.
   *
   * <p><b>IMPORTANT:</b> topic deletion is asynchronous. Attempts to recreate the same topic may
   * fail for a short period of time.  Avoid recreating topics with the same name if at all
   * possible. Where this is not possible, wrap the creation in a retry loop.
   *
   * @param topics the topics to delete.
   */
  void deleteTopics(final Collection<String> topics) {
    try (Admin adminClient = adminClient()) {

      adminClient.deleteTopics(topics).all().get();

      // Deletion is async: wait util they are no longer reported
      final Set<String> remaining = new HashSet<>(topics);
      while (!remaining.isEmpty()) {
        final Set<String> topicNames = adminClient.listTopics().names().get();
        remaining.retainAll(topicNames);
      }

    } catch (final Exception e) {
      throw new RuntimeException("Failed to delete topics: " + topics, e);
    }
  }

  /**
   * Wait for topics with names {@code topicNames} to not exist in Kafka.
   *
   * @param topicNames the names of the topics to await absence for.
   */
  void waitForTopicsToBePresent(final String... topicNames) {
    try (Admin adminClient = adminClient()) {
      final Set<String> required = ImmutableSet.copyOf(topicNames);

      final Supplier<Collection<String>> remaining = () -> {
        final Set<String> names = getTopicNames(adminClient);
        names.retainAll(required);
        return names;
      };

      assertThatEventually(
          "topics not all prresent after timeout",
          remaining,
          is(required)
      );
    }
  }

  /**
   * Wait for topics with names {@code topicNames} to not exist in Kafka.
   *
   * @param topicNames the names of the topics to await absence for.
   */
  void waitForTopicsToBeAbsent(final String... topicNames) {
    try (Admin adminClient = adminClient()) {

      final Supplier<Collection<String>> remaining = () -> {
        final Set<String> names = getTopicNames(adminClient);
        names.retainAll(Arrays.asList(topicNames));
        return names;
      };

      assertThatEventually(
          "topics not all absent after timeout",
          remaining,
          is(empty())
      );
    }
  }


  /**
   * Gets the mapping of TopicPartitions to current consumed offset for a given consumer group.
   * @param consumerGroup The consumer group to read the offset for
   * @return The mapping of TopicPartitions to current consumed offsets
   */
  public Map<TopicPartition, Long> getConsumerGroupOffset(final String consumerGroup) {
    try (AdminClient adminClient = adminClient()) {
      final ListConsumerGroupOffsetsResult result =
          adminClient.listConsumerGroupOffsets(consumerGroup);
      final Map<TopicPartition, OffsetAndMetadata> metadataMap =
          result.partitionsToOffsetAndMetadata().get();
      return metadataMap.entrySet().stream()
          .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().offset()));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * The end offsets for a given collection of TopicPartitions
   * @param topicPartitions The collection of TopicPartitions to get end offsets for
   * @param isolationLevel The isolation level to use when reading end offsets.
   * @return The map of TopicPartition to end offset
   */
  public Map<TopicPartition, Long> getEndOffsets(
      final Collection<TopicPartition> topicPartitions,
      final IsolationLevel isolationLevel) {
    final Map<TopicPartition, OffsetSpec> topicPartitionsWithSpec = topicPartitions.stream()
        .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
    try (AdminClient adminClient = adminClient()) {
      final ListOffsetsResult listOffsetsResult = adminClient.listOffsets(
          topicPartitionsWithSpec, new ListOffsetsOptions(isolationLevel));
      final Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> partitionResultMap =
          listOffsetsResult.all().get();
      return partitionResultMap
          .entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the partition count for a given collection of topics.
   * @param topics The collection of topics to lookup
   * @return The mapping of topic to partition count
   */
  public Map<String, Integer> getPartitionCount(final Collection<String> topics) {
    try (AdminClient adminClient = adminClient()) {
      final DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topics);
      final Map<String, TopicDescription> topicDescriptionMap =
          describeTopicsResult.allTopicNames().get();
      return topicDescriptionMap
          .entrySet()
          .stream()
          .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().partitions().size()));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Set<String> getTopicNames(final Admin adminClient) {
    try {
      return adminClient.listTopics().names().get();
    } catch (final Exception e) {
      throw new RuntimeException("Failed to get topic names", e);
    }
  }

  private String zookeeperConnect() {
    return config.getProperty(KafkaConfig.ZkConnectProp());
  }

  private String logDir() {
    return config.getProperty(KafkaConfig.LogDirProp());
  }

  private AdminClient adminClient() {
    final ImmutableMap<String, Object> props = ImmutableMap.of(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList(),
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000);

    return AdminClient.create(props);
  }
}