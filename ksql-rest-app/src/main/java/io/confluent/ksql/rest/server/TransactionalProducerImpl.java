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
import com.google.common.io.Closer;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.rest.util.InternalTopicJsonSerdeUtil;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;


/**
 * Used to handle transactional produces to the command topic. Ensures that commands are only 
 * produced to the command topic if the server is in sync (commandRunner has processed all available
 * records in the command topic). Consists of a transactional producer and consumer.
 */
@NotThreadSafe
public class TransactionalProducerImpl implements TransactionalProducer {

  private static final int COMMAND_TOPIC_PARTITION = 0;
  private final TopicPartition commandTopicPartition;
  private final String commandTopicName;
  
  private final Consumer<CommandId, Command> commandConsumer;
  private final Producer<CommandId, Command> commandProducer;
  private final Duration commandQueueCatchupTimeout;
  private final CommandStore commandStore;
  private final Closer closer;

  public TransactionalProducerImpl(
      final String commandTopicName,
      final CommandStore commandStore,
      final Duration commandQueueCatchupTimeout,
      final Map<String, Object> kafkaConsumerProperties,
      final Map<String, Object> kafkaProducerProperties
  ) {
    this(
        commandTopicName,
        commandStore,
        commandQueueCatchupTimeout,
        new KafkaConsumer<>(
          Objects.requireNonNull(kafkaConsumerProperties, "kafkaConsumerProperties"),
          InternalTopicJsonSerdeUtil.getJsonDeserializer(CommandId.class, true),
          InternalTopicJsonSerdeUtil.getJsonDeserializer(Command.class, false)
        ),
        new KafkaProducer<>(
          Objects.requireNonNull(kafkaProducerProperties, "kafkaProducerProperties"),
          InternalTopicJsonSerdeUtil.getJsonSerializer(true),
          InternalTopicJsonSerdeUtil.getJsonSerializer(false)
        )
    );
  }

  @VisibleForTesting
  TransactionalProducerImpl(
      final String commandTopicName,
      final CommandStore commandRunner,
      final Duration commandQueueCatchupTimeout,
      final Consumer<CommandId, Command> commandConsumer,
      final Producer<CommandId, Command> commandProducer
  ) {
    this.commandTopicPartition = new TopicPartition(
        Objects.requireNonNull(commandTopicName, "commandTopicName"),
        COMMAND_TOPIC_PARTITION
    );
    this.commandQueueCatchupTimeout = 
        Objects.requireNonNull(commandQueueCatchupTimeout, "commandQueueCatchupTimeout");
    this.commandConsumer = Objects.requireNonNull(commandConsumer, "commandConsumer");
    this.commandProducer = Objects.requireNonNull(commandProducer, "commandProducer");
    this.commandTopicName = Objects.requireNonNull(commandTopicName, "commandTopicName");
    this.commandStore = Objects.requireNonNull(commandRunner, "commandRunner");
    this.closer = Closer.create();
    closer.register(commandConsumer);
    closer.register(commandProducer);
  }

  /**
   * Initializes the transactional producer and assigns the consumer to the command topic partition.
   * Must be called before any other method.
   */
  public void initialize() {
    commandProducer.initTransactions();
    commandConsumer.assign(Collections.singleton(commandTopicPartition));
  }

  /** 
   * Begins the transaction and waits for the {@link CommandRunner} to process all
   * available records in the command topic
   */
  public void begin() {
    commandProducer.beginTransaction();
    waitForConsumer();
  }

  /**
   * Helper function that blocks until the commandStore has consumed past the current
   * end offset of the command topic and the commandRunner has processed those records.
   */
  private void waitForConsumer() {
    try {
      final long endOffset = getEndOffset();

      commandStore.ensureConsumedPast(endOffset - 1, commandQueueCatchupTimeout);
    } catch (final InterruptedException e) {
      final String errorMsg = 
          "Interrupted while waiting for commandRunner to process command topic";
      throw new KsqlRestException(
          Errors.serverErrorForStatement(e, errorMsg, new KsqlEntityList()));
    } catch (final TimeoutException e) {
      final String errorMsg =
          "Timeout while waiting for commandRunner to process command topic";
      throw new KsqlRestException(
          Errors.serverErrorForStatement(e, errorMsg, new KsqlEntityList()));
    }
  }
  
  private long getEndOffset() {
    return commandConsumer.endOffsets(Collections.singletonList(commandTopicPartition))
        .get(commandTopicPartition);
  }

  public RecordMetadata send(final CommandId commandId, final Command command) {
    final ProducerRecord<CommandId, Command> producerRecord = new ProducerRecord<>(
        commandTopicName,
        COMMAND_TOPIC_PARTITION,
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
      throw new KafkaException(e);
    }
  }

  public void abort() {
    commandProducer.abortTransaction();
  }

  public void commit() {
    commandProducer.commitTransaction();
  }

  public void close() throws IOException {
    closer.close();
  }
}
