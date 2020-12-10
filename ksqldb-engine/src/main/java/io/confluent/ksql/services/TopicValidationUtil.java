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
import org.apache.kafka.clients.admin.TopicDescription;

final class TopicValidationUtil {

  private TopicValidationUtil() {

  }

  public static void validateTopicProperties(
      final int requiredNumPartition,
      final int requiredNumReplicas,
      final TopicDescription existingTopic
  ) {
    final int actualNumPartitions = existingTopic.partitions().size();
    final int actualNumReplicas = existingTopic.partitions().get(0).replicas().size();

    final String topicName = existingTopic.name();
    validateTopicProperties(
        topicName,
        requiredNumPartition,
        requiredNumReplicas,
        actualNumPartitions,
        actualNumReplicas);
  }

  public static void validateTopicProperties(
      final String topicName,
      final int requiredNumPartition,
      final int requiredNumReplicas,
      final int actualNumPartitions,
      final int actualNumReplicas
  ) {
    if (actualNumPartitions != requiredNumPartition
        || (requiredNumReplicas != TopicProperties.DEFAULT_REPLICAS
        && actualNumReplicas < requiredNumReplicas)) {
      throw new KafkaTopicExistsException(String.format(
          "A Kafka topic with the name '%s' already exists, with different partition/replica "
              + "configuration than required. KSQL expects %d partitions (topic has %d), and %d "
              + "replication factor (topic has %d).",
          topicName,
          requiredNumPartition,
          actualNumPartitions,
          requiredNumReplicas,
          actualNumReplicas
      ), true);
    }
  }
}
