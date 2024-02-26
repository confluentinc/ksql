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
import java.util.Optional;
import org.apache.kafka.clients.admin.TopicDescription;

final class TopicValidationUtil {

  private TopicValidationUtil() {

  }

  public static void validateTopicProperties(
      final int requiredNumPartition,
      final int requiredNumReplicas,
      final Optional<Long> requiredRetentionMs,
      final TopicDescription existingTopic,
      final Map<String, String> existingConfig
  ) {
    final int actualNumPartitions = existingTopic.partitions().size();
    final int actualNumReplicas = existingTopic.partitions().get(0).replicas().size();
    final Optional<Long> actualRetentionMs = KafkaTopicClient.getRetentionMs(existingConfig);

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
      final Optional<Long> requiredRetentionMs,
      final int actualNumPartitions,
      final int actualNumReplicas,
      final Optional<Long> actualRetentionMs
  ) {
    if (isInvalidPartitions(actualNumPartitions, requiredNumPartition)
        || isInvalidReplicas(actualNumReplicas, requiredNumReplicas)
        || isInvalidRetention(actualRetentionMs, requiredRetentionMs)) {
      String errMsg = String.format(
          "A Kafka topic with the name '%s' already exists, with different partition/replica"
          + " configuration than required. KSQL expects %d partitions (topic has %d),"
          + " %d replication factor (topic has %d)",
          topicName,
          requiredNumPartition,
          actualNumPartitions,
          requiredNumReplicas,
          actualNumReplicas);
      if (requiredRetentionMs.isPresent() && actualRetentionMs.isPresent()) {
        errMsg = errMsg.replace("partition/replica", "partition/replica/retention");
        errMsg = String.format(errMsg + ", and %d retention (topic has %d).",
            requiredRetentionMs.get(), actualRetentionMs.get());
      } else {
        errMsg += ".";
      }
      throw new KafkaTopicExistsException(errMsg, true);
    }
  }

  private static boolean isInvalidPartitions(final int actualNumPartitions,
      final int requiredNumPartition) {
    return actualNumPartitions != requiredNumPartition;
  }

  private static boolean isInvalidReplicas(final int actualNumReplicas,
      final int requiredNumReplicas) {
    return requiredNumReplicas != TopicProperties.DEFAULT_REPLICAS
        && actualNumReplicas < requiredNumReplicas;
  }

  private static boolean isInvalidRetention(final Optional<Long> actualRetentionMs,
      final Optional<Long> requiredRetentionMs) {
    return requiredRetentionMs.isPresent()
        && actualRetentionMs.isPresent()
        && actualRetentionMs.get().longValue() != requiredRetentionMs.get().longValue();
  }

}
