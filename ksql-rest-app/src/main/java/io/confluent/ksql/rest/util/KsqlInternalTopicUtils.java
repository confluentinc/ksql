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

package io.confluent.ksql.rest.util;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.exception.KafkaTopicExistsException;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlInternalTopicUtils {
  private static final Logger log = LoggerFactory.getLogger(KsqlInternalTopicUtils.class);

  private static final int NPARTITIONS = 1;

  public static String getTopicName(final KsqlConfig ksqlConfig, final String topicSuffix) {
    return String.format(
        "%s%s_%s",
        KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
        topicSuffix
    );
  }

  public static void ensureTopic(final String name,
                                 final KsqlConfig ksqlConfig,
                                 final KafkaTopicClient topicClient) {
    ensureTopic(name, ksqlConfig, topicClient, TopicConfig.CLEANUP_POLICY_DELETE);
  }

  public static void ensureTopic(final String name,
                                 final KsqlConfig ksqlConfig,
                                 final KafkaTopicClient topicClient,
                                 final String cleanupPolicy) {
    final long requiredTopicRetention = Long.MAX_VALUE;
    if (topicClient.isTopicExists(name)) {
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

    try {
      final short replicationFactor =
          ksqlConfig.originals().containsKey(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY)
            ? ksqlConfig.getShort(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY) : 1;
      if (replicationFactor < 2) {
        log.warn("Creating topic {} with replication factor of {} which is less than 2. "
                + "This is not advisable in a production environment. ",
            name, replicationFactor);
      }

      topicClient.createTopic(
          name,
          NPARTITIONS,
          replicationFactor,
          ImmutableMap.of(
              TopicConfig.RETENTION_MS_CONFIG, requiredTopicRetention,
              TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy)
      );
    } catch (final KafkaTopicExistsException e) {
      log.info("Internal Topic {} Exists: {}", name, e.getMessage());
    }
  }
}
