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

import io.confluent.ksql.exception.KafkaTopicExistsException;
import io.confluent.ksql.topic.TopicProperties;
import java.util.Map;
import org.apache.kafka.clients.admin.TopicDescription;

final class TopicValidationUtil {

  private TopicValidationUtil() {

  }

  public static void validateTopicProperties(
      final int requiredNumPartition,
      final int requiredNumReplicas,
      final long requiredRetentionMs,
      final TopicDescription existingTopic,
      final Map<String, String> existingConfig
  ) {
    final int actualNumPartitions = existingTopic.partitions().size();
    final int actualNumReplicas = existingTopic.partitions().get(0).replicas().size();
    final long actualRetentionMs = KafkaTopicClient.getRetentionMs(existingConfig);

    final String topicName = existingTopic.name();
    validateTopicProperties(
        topicName,
        requiredNumPartition,
        requiredNumReplicas,
        requiredRetentionMs,
        actualNumPartitions,
        actualNumReplicas,
        actualRetentionMs);
  }

  public static void validateTopicProperties(
      final String topicName,
      final int requiredNumPartition,
      final int requiredNumReplicas,
      final long requiredRetentionMs,
      final int actualNumPartitions,
      final int actualNumReplicas,
      final long actualRetentionMs
  ) {
    if (actualNumPartitions != requiredNumPartition
        || (requiredNumReplicas != TopicProperties.DEFAULT_REPLICAS
        && actualNumReplicas < requiredNumReplicas)
        || isValidRetention(actualRetentionMs, requiredRetentionMs)) {
      throw new KafkaTopicExistsException(String.format(
          "A Kafka topic with the name '%s' already exists, with different partition/replica"
              + "/retention configuration than required. KSQL expects %d partitions (topic has %d),"
              + " %d replication factor (topic has %d), and %d retention (topic has %d).",
          topicName,
          requiredNumPartition,
          actualNumPartitions,
          requiredNumReplicas,
          actualNumReplicas,
          requiredRetentionMs,
          actualRetentionMs
      ), true);
    }
  }

  private static boolean isValidRetention(
      final long actualRetentionMs,
      final long requiredRetentionMs
  ) {
    return requiredRetentionMs != -1 && actualRetentionMs != requiredRetentionMs;
  }

}
