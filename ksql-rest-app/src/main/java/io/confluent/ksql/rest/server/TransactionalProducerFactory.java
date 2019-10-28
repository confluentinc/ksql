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

package io.confluent.ksql.rest.server;

import io.confluent.ksql.rest.server.computation.CommandRunner;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerConfig;

public class TransactionalProducerFactory {
  private final String commandTopicName;
  private final CommandRunner commandRunner;
  private final Map<String, Object> kafkaConsumerProperties;
  private final Map<String, Object> kafkaProducerProperties;

  public TransactionalProducerFactory(
      final String commandTopicName,
      final String transactionId,
      final CommandRunner commandRunner,
      final Map<String, Object> kafkaConsumerProperties,
      final Map<String, Object> kafkaProducerProperties
  ) {
    this.kafkaConsumerProperties =
        Objects.requireNonNull(kafkaConsumerProperties, "kafkaConsumerProperties");
    this.kafkaProducerProperties =
        Objects.requireNonNull(kafkaProducerProperties, "kafkaProducerProperties");
    this.commandTopicName = Objects.requireNonNull(commandTopicName, "commandTopicName");
    this.commandRunner = Objects.requireNonNull(commandRunner, "commandRunner");

    kafkaProducerProperties.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
        Objects.requireNonNull(transactionId, "transactionId")
    );
  }

  public TransactionalProducer createProducerTransactionManager() {
    return new TransactionalProducer(
        commandTopicName,
        commandRunner,
        kafkaConsumerProperties,
        kafkaProducerProperties
    );
  }
}
