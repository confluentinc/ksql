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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.util.InternalTopicJsonSerdeUtil;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

/**
 * Used to handle transactional writes to the command topic
 */
public class ProducerTransactionManager {

  private final TopicPartition commandTopicPartition;
  private final String commandTopicName;

  private final Consumer<CommandId, Command> commandConsumer;
  private final Producer<CommandId, Command> commandProducer;
  private final CommandRunner commandRunner;

  public ProducerTransactionManager(
      final String commandTopicName,
      final CommandRunner commandRunner,
      final Map<String, Object> kafkaConsumerProperties,
      final Map<String, Object> kafkaProducerProperties
  ) {
    this.commandTopicPartition = new TopicPartition(
            Objects.requireNonNull(commandTopicName, "commandTopicName"),
            0
    );

    this.commandConsumer = new KafkaConsumer<>(
        Objects.requireNonNull(kafkaConsumerProperties, "kafkaConsumerProperties"),
        InternalTopicJsonSerdeUtil.getJsonDeserializer(CommandId.class, true),
        InternalTopicJsonSerdeUtil.getJsonDeserializer(Command.class, false)
    );

    this.commandProducer = new KafkaProducer<>(
        Objects.requireNonNull(kafkaProducerProperties, "kafkaProducerProperties"),
        InternalTopicJsonSerdeUtil.getJsonSerializer(true),
        InternalTopicJsonSerdeUtil.getJsonSerializer(false)
    );
    this.commandTopicName = Objects.requireNonNull(commandTopicName, "commandTopicName");
    this.commandRunner = Objects.requireNonNull(commandRunner, "commandRunner");
  }

  @VisibleForTesting
  ProducerTransactionManager(
      final String commandTopicName,
      final CommandRunner commandRunner,
      final Consumer<CommandId, Command> commandConsumer,
      final Producer<CommandId, Command> commandProducer
  ) {
    this.commandTopicPartition = new TopicPartition(
        Objects.requireNonNull(commandTopicName, "commandTopicName"),
        0
    );
    this.commandConsumer = Objects.requireNonNull(commandConsumer, "commandConsumer");
    this.commandProducer = Objects.requireNonNull(commandProducer, "commandProducer");
    this.commandTopicName = Objects.requireNonNull(commandTopicName, "commandTopicName");
    this.commandRunner = Objects.requireNonNull(commandRunner, "commandRunner");
  }


  /** begins transaction */
  public void begin() {
    commandConsumer.assign(Collections.singleton(commandTopicPartition));
    commandProducer.initTransactions();
    commandProducer.beginTransaction();
  }

  public void waitForCommandRunner() {
    final long endOffset = getEndOffset();

    try {
      int retries = 0;
      while (commandRunner.getNumCommandProcessed() < endOffset) {
        Thread.sleep(1000);

        if (retries == 60) {
          throw new RuntimeException("commandRunner has not processed all commands in topic");
        }
        retries++;
      }
    } catch (Exception exception) {
      throw new RuntimeException(
              "Error while waiting for commandRunner to process command topic:",
              exception
      );
    }
  }

  private long getEndOffset() {
    return commandConsumer.endOffsets(Collections.singletonList(commandTopicPartition))
        .get(commandTopicPartition);
  }

  public RecordMetadata send(final CommandId commandId, final Command command) {
    final ProducerRecord<CommandId, Command> producerRecord = new ProducerRecord<>(
        commandTopicName,
        0,
        Objects.requireNonNull(commandId, "commandId"),
        Objects.requireNonNull(command, "command"));
    try {
      return commandProducer.send(producerRecord).get();
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    } catch (KafkaException e) {
      commandProducer.abortTransaction();
      throw new KafkaException(e);
    }
  }

  public void abort() {
    commandProducer.abortTransaction();
    close();
    
  }

  public void commit() {
    commandProducer.commitTransaction();
    close();
  }

  public void close() {
    commandConsumer.close();
    commandProducer.close();
  }
}
