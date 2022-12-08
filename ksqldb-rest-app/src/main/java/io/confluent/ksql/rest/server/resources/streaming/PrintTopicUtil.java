/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.services.ConfiguredKafkaClientSupplier;
import io.confluent.ksql.services.ServiceContext;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaClientSupplier;

public final class PrintTopicUtil {

  private PrintTopicUtil() {
  }

  public static KafkaConsumer<Bytes, Bytes> createTopicConsumer(
          final ServiceContext serviceContext,
          final Map<String, Object> consumerProperties,
          final PrintTopic printTopic
  ) {
    final KafkaConsumer<Bytes, Bytes> topicConsumer = new KafkaConsumer<>(
        injectSupplierProperties(serviceContext, consumerProperties),
        new BytesDeserializer(),
        new BytesDeserializer()
    );

    final List<TopicPartition> topicPartitions =
        topicConsumer.partitionsFor(printTopic.getTopic())
            .stream()
            .map(partitionInfo ->
                new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            .collect(Collectors.toList());
    topicConsumer.assign(topicPartitions);

    if (printTopic.getFromBeginning()) {
      topicConsumer.seekToBeginning(topicPartitions);
    }
    return topicConsumer;
  }

  private static Map<String, Object> injectSupplierProperties(
      final ServiceContext serviceContext,
      final Map<String, Object> consumerProperties
  ) {
    final KafkaClientSupplier kafkaClientSupplier = serviceContext.getKafkaClientSupplier();

    // If the KafkaClientSupplier has specific properties, then inject them here so we can
    // create a KafkaConsumer with the properties specified
    if (kafkaClientSupplier instanceof ConfiguredKafkaClientSupplier) {
      return ((ConfiguredKafkaClientSupplier) kafkaClientSupplier)
          .injectSupplierProperties(consumerProperties);
    }

    return consumerProperties;
  }
}
