/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;


public class BrokerCompatibilityCheck implements Closeable {

  private static final String KSQL_COMPATIBILITY_CHECK = "_confluent-ksql_compatibility_check";
  private final Consumer<?, ?> consumer;
  private final TopicPartition topicPartition;

  BrokerCompatibilityCheck(
      final Consumer<?, ?> consumer,
      final TopicPartition topicPartition
  ) {
    this.consumer = consumer;
    this.topicPartition = topicPartition;
  }

  public static BrokerCompatibilityCheck create(
      final Map<String, Object> streamsConfig,
      final KafkaTopicClient topicClient
  ) {
    Set<String> topicNames = topicClient.listTopicNames();
    // the offsetsForTime call needs a partition that exists else it can block forever
    if (topicNames.isEmpty()) {
      topicClient.createTopic(KSQL_COMPATIBILITY_CHECK, 1, (short) 1);
      topicNames = Utils.mkSet(KSQL_COMPATIBILITY_CHECK);
    }
    final Map<String, Object> consumerConfigs = new StreamsConfig(streamsConfig)
        .getConsumerConfigs(KSQL_COMPATIBILITY_CHECK, "ksql_server");

    // remove this otherwise it will try and instantiate the StreamsPartitionAssignor
    consumerConfigs.remove("partition.assignment.strategy");
    final KafkaConsumer<String, String> consumer
        = new KafkaConsumer<>(consumerConfigs, new StringDeserializer(), new StringDeserializer());
    return new BrokerCompatibilityCheck(
        consumer,
        new TopicPartition(topicNames.iterator().next(), 0)
    );
  }

  /**
   * Check if the used brokers have version 0.10.1.x or higher.
   *
   * <p>Note during an upgrade this check may pass or fail depending on
   * which broker is the leader for the partition, i.e, if an old broker is the leader
   * the check would fail. If a new broker is the leader it will pass. This is only
   * a temporary situation and will naturally rectify itself.
   *
   * @throws KsqlException if brokers have version 0.10.0.x
   */
  void checkCompatibility() throws StreamsException {
    try {
      consumer.offsetsForTimes(Collections.singletonMap(topicPartition, 0L));
    } catch (final UnsupportedVersionException e) {
      throw new KsqlException(
          "The kafka brokers are incompatible with. "
          + "KSQL requires broker versions >= 0.10.1.x"
      );
    }
  }

  public void close() {
    consumer.close();
  }
}
