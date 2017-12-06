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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import io.confluent.ksql.util.KsqlException;


public class BrokerCompatibilityCheck implements Closeable {

  private final Consumer<?, ?> consumer;
  private final TopicPartition topicPartition;

  BrokerCompatibilityCheck(final Consumer<?, ?> consumer,
                           final TopicPartition topicPartition) {
    this.consumer = consumer;
    this.topicPartition = topicPartition;
  }


  public static BrokerCompatibilityCheck create(final Map<String, Object> streamsConfig, final Set<String> topicNames) {
    // the offsetsForTime call needs a partition that exists else it can block forever
    if (topicNames.isEmpty()) {
      throw new KsqlException("Unable to check broker compatibility against a broker without any topics");
    }
    final Map<String, Object> consumerConfigs = new StreamsConfig(streamsConfig)
        .getConsumerConfigs("__ksql_compatibility_check", "ksql_server");

    // remove this otherwise it will try and instantiate the StreamsPartitionAssignor
    consumerConfigs.remove("partition.assignment.strategy");
    final KafkaConsumer<String, String> consumer
        = new KafkaConsumer<>(consumerConfigs, new StringDeserializer(), new StringDeserializer());
    return new BrokerCompatibilityCheck(consumer,  new TopicPartition(topicNames.iterator().next(), 0));
  }

  /**
   * Check if the used brokers have version 0.10.1.x or higher.
   *
   * @throws KsqlException if brokers have version 0.10.0.x
   */
  void checkCompatibility() throws StreamsException {
    try {
      consumer.offsetsForTimes(Collections.singletonMap(topicPartition, 0L));
    } catch (final UnsupportedVersionException e) {
      throw new KsqlException("The kafka brokers are incompatible with. "
           + "KSQL requires broker versions >= 0.10.1.x");
    }
  }

  public void close() {
    consumer.close();
  }
}
