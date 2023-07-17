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

package io.confluent.ksql.rest.util;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KsqlInternalTopicUtils provides some utility functions for working with internal topics.
 * An internal topic is a topic that the KSQL server uses to manage its own internal state.
 * This is separate from KSQL sink topics, and topics internal to individual queries
 * (e.g. repartition and changelog topics). Some examples are the command topic used by
 * interactive KSQL, and the config topic used by headless KSQL.
 */
public final class KsqlInternalTopicUtils {

  private static final Logger log = LoggerFactory.getLogger(KsqlInternalTopicUtils.class);

  private static final int INTERNAL_TOPIC_PARTITION_COUNT = 1;
  private static final long INTERNAL_TOPIC_RETENTION_MS = -1;

  private static final ImmutableMap<String, ?> INTERNAL_TOPIC_CONFIG = ImmutableMap.of(
      TopicConfig.RETENTION_MS_CONFIG, INTERNAL_TOPIC_RETENTION_MS,
      TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
      TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, false
  );

  private KsqlInternalTopicUtils() {
  }

  /**
   * Ensure that an internal topic exists with the requested configuration, creating it
   * if necessary. This function will also fix the retention time on topics if it detects
   * that retention is not infinite.
   *
   * @param name The name of the internal topic to ensure.
   * @param ksqlConfig The KSQL config, which contains properties that are translated
   *                   to topic configs.
   * @param topicClient A topic client used to query topic configs and create the topic.
   */
  public static void ensureTopic(
      final String name,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient topicClient
  ) {
    if (topicClient.isTopicExists(name)) {
      validateTopicConfig(name, ksqlConfig, topicClient);
      return;
    }

    final short replicationFactor = ksqlConfig
        .getShort(KsqlConfig.KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY);

    if (replicationFactor < 2) {
      log.warn("Creating topic {} with replication factor of {} which is less than 2. "
              + "This is not advisable in a production environment. ",
          name, replicationFactor);
    }

    final short minInSyncReplicas = ksqlConfig
        .getShort(KsqlConfig.KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS_PROPERTY);

    topicClient.createTopic(
        name,
        INTERNAL_TOPIC_PARTITION_COUNT,
        replicationFactor,
        ImmutableMap.<String, Object>builder()
            .putAll(INTERNAL_TOPIC_CONFIG)
            .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSyncReplicas)
            .build()
    );
  }

  private static void validateTopicConfig(
      final String name,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient topicClient
  ) {
    final TopicDescription description = topicClient.describeTopic(name);

    final int actualPartitionCount = description.partitions().size();
    if (actualPartitionCount != INTERNAL_TOPIC_PARTITION_COUNT) {
      throw new IllegalStateException("Invalid partition count on topic "
          + "'" + name + "'. "
          + "Expected: " + INTERNAL_TOPIC_PARTITION_COUNT + ", but was: " + actualPartitionCount);
    }

    final short replicationFactor = ksqlConfig
        .getShort(KsqlConfig.KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY);

    final int actualRf = description.partitions().get(0).replicas().size();
    if (actualRf < replicationFactor) {
      throw new IllegalStateException("Invalid replication factor on topic "
          + "'" + name + "'. "
          + "Expected: " + replicationFactor + ", but was: " + actualRf);
    }

    if (replicationFactor < 2) {
      log.warn("Topic {} has replication factor of {} which is less than 2. "
              + "This is not advisable in a production environment. ",
          name, replicationFactor);
    }

    final Map<String, String> existingConfig = topicClient.getTopicConfig(name);
    if (topicClient.addTopicConfig(name, INTERNAL_TOPIC_CONFIG)) {
      log.warn(
          "Topic {} was created with or modified to have an invalid configuration: {} "
              + "- overriding the following configurations: {}",
          name,
          existingConfig,
          INTERNAL_TOPIC_CONFIG);
    }
  }
}
