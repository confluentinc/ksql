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
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
 * default.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
class KafkaEmbedded {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LogManager.getLogger(KafkaEmbedded.class);

  private final Properties config = null;
  private final KafkaClusterTestKit cluster;

  /**
   * Creates and starts an embedded Kafka broker.
   *
   * @param config Broker configuration settings.  Used to modify, for example, the listeners
   *               the broker should use.  Note that you cannot change some settings such as
   *               `log.dirs`.
   */
  KafkaEmbedded(final Map<String, String> config) {
    log.debug("Starting embedded Kafka broker");

    applyDefaultConfig(config);
    setupListenerConfiguration(config);

    final Map<Integer, Map<String,String>> brokerConfigs = new HashMap<>();
    brokerConfigs.put(0, config);

    try {
      final KafkaClusterTestKit.Builder clusterBuilder = new KafkaClusterTestKit.Builder(
              new TestKitNodes.Builder()
                      .setCombined(true)
                      .setNumBrokerNodes(1)
                      .setPerServerProperties(brokerConfigs)
                      .setNumControllerNodes(1)
                      .build()
      );

      cluster = clusterBuilder.build();
      // cluster.nonFatalFaultHandler().setIgnore(true);

      cluster.format();
      cluster.startup();
      cluster.waitForReadyBrokers();
    } catch (final Exception e) {
      throw new KafkaException("Failed to create test Kafka cluster", e);
    }
    log.debug("Startup of embedded Kafka broker at {} completed  ...", brokerList());
  }

  /**
   * Apply basic broker configurations.
   *
   * @param config The broker configuration settings to be updated.
   */
  private void applyDefaultConfig(final Map<String, String> config) {
    // Basic broker settings
    config.put("broker.id", "0");
    config.put("num.partitions", "1");
    config.put("auto.create.topics.enable", "true");
    config.put("message.max.bytes", "1000000");
    config.put("controlled.shutdown.enable", "true");

    // License validator overrides for tests
    config.put("confluent.license.validator.enabled", "false");
    config.put("confluent.license.topic.auto.create", "false");
    config.put("confluent.license", "test");
    config.put("confluent.license.topic.replication.factor", "1");
  }

  /**
   * Setup listener configuration for KRaft mode with dynamic free ports.
   *
   * @param config The broker configuration settings to be updated.
   */
  private void setupListenerConfiguration(final Map<String, String> config) {
    final int controllerPort = getFreePort();
    final int externalPort = getFreePort();

    config.put("listeners", "CONTROLLER://127.0.0.1:" + controllerPort
        + ",EXTERNAL://127.0.0.1:" + externalPort);
    config.put("inter.broker.listener.name", "EXTERNAL");
    config.put("controller.listener.names", "CONTROLLER");
    config.put("advertised.listeners", "EXTERNAL://127.0.0.1:" + externalPort);

    if (!config.containsKey("listener.security.protocol.map")) {
      // Default to PLAINTEXT if not set
      config.put("listener.security.protocol.map", "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT");
    }
  }

  private static int getFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException("Unable to get a free port", e);
    }
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
    return cluster.bootstrapServers();
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
    throw new RuntimeException("No listener with protocol " + securityProtocol);
  }

  /**
   * Stop the broker.
   */
  void stop() {
    log.debug("Shutting down embedded Kafka broker at {} ...", brokerList());
    try {
      cluster.close();
    } catch (RejectedExecutionException ree) {
      // The executor is already terminated; log a warning and continue
      log.warn("Executor already terminated; ignoring shutdown exception", ree);
    } catch (Exception e) {
      throw new KafkaException("Failed to shutdown Kafka cluster", e);
    }
    log.debug("Shutdown of embedded Kafka broker at {} completed ...", brokerList());
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
          "topics not all present after timeout",
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

  private AdminClient adminClient() {
    final ImmutableMap<String, Object> props = ImmutableMap.of(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList(),
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 60_000);

    return AdminClient.create(props);
  }
}