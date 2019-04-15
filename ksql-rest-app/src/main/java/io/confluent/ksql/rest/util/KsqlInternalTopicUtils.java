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
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
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

  private static final int NPARTITIONS = 1;

  private KsqlInternalTopicUtils() {
  }

  /**
   * Compute a name for an internal topic.
   *
   * @param ksqlConfig The KSQL config, which is used to extract the internal topic prefix.
   * @param topicSuffix A suffix that is appended to the topic name.
   * @return The computed topic name.
   */
  public static String getTopicName(final KsqlConfig ksqlConfig, final String topicSuffix) {
    return String.format(
        "%s%s_%s",
        KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
        topicSuffix
    );
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
  public static void ensureTopic(final String name,
                                 final KsqlConfig ksqlConfig,
                                 final KafkaTopicClient topicClient) {
    final short replicationFactor =
        ksqlConfig.originals().containsKey(KsqlConfig.KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY)
            ? ksqlConfig.getShort(KsqlConfig.KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY) : 1;
    if (replicationFactor < 2) {
      log.warn("Creating topic {} with replication factor of {} which is less than 2. "
              + "This is not advisable in a production environment. ",
          name, replicationFactor);
    }
    final long requiredTopicRetention = Long.MAX_VALUE;

    if (topicClient.isTopicExists(name)) {
      final TopicDescription description = topicClient.describeTopic(name);
      if (description.partitions().size() != NPARTITIONS) {
        throw new IllegalStateException(
            String.format(
                "Invalid partition count on topic %s: %d", name, description.partitions().size()));
      }
      final int nReplicas = description.partitions().get(0).replicas().size();
      if (nReplicas < replicationFactor) {
        throw new IllegalStateException(
            String.format(
                "Invalid replcation factor on topic %s: %d", name, nReplicas));
      }

      final ImmutableMap<String, Object> requiredConfig =
          ImmutableMap.of(TopicConfig.RETENTION_MS_CONFIG, requiredTopicRetention);

      if (topicClient.addTopicConfig(name, requiredConfig)) {
        log.info(
            "Corrected retention.ms on ksql internal topic. topic:{}, retention.ms:{}",
            name,
            requiredTopicRetention);
      }

      return;
    }

    topicClient.createTopic(
        name,
        NPARTITIONS,
        replicationFactor,
        ImmutableMap.of(
            TopicConfig.RETENTION_MS_CONFIG, requiredTopicRetention,
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
    );
  }
}
