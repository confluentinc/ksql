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

package io.confluent.ksql.rest.server.computation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.CommandTopic;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.QueryMask;
import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

/**
 * Wrapper class for the command topic. Used for reading from the topic (either all messages from
 * the beginning until now, or any new messages since then), and writing to it.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class CommandStore implements CommandQueue, Closeable {

  private static final Duration POLLING_TIMEOUT_FOR_COMMAND_TOPIC = Duration.ofMillis(5000);
  private static final int COMMAND_TOPIC_PARTITION = 0;

  private final CommandTopic commandTopic;
  private final Map<CommandId, CommandStatusFuture> commandStatusMap;
  private final SequenceNumberFutureStore sequenceNumberFutureStore;

  private final String commandTopicName;
  private final Duration commandQueueCatchupTimeout;
  private final Map<String, Object> kafkaConsumerProperties;
  private final Map<String, Object> kafkaProducerProperties;

  public static final class Factory {

    private Factory() {
    }

    public static CommandStore create(
        final String commandTopicName,
        final String transactionId,
        final Duration commandQueueCatchupTimeout,
        final Map<String, Object> kafkaConsumerProperties,
        final Map<String, Object> kafkaProducerProperties
    ) {
      kafkaConsumerProperties.put(
          ConsumerConfig.ISOLATION_LEVEL_CONFIG,
          IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT)
      );
      kafkaProducerProperties.put(
          ProducerConfig.TRANSACTIONAL_ID_CONFIG,
          transactionId
      );
      kafkaProducerProperties.put(
          ProducerConfig.ACKS_CONFIG,
          "all"
      );

      return new CommandStore(
          commandTopicName,
          new CommandTopic(commandTopicName, kafkaConsumerProperties),
          new SequenceNumberFutureStore(),
          kafkaConsumerProperties,
          kafkaProducerProperties,
          commandQueueCatchupTimeout
      );
    }
  }

  CommandStore(
      final String commandTopicName,
      final CommandTopic commandTopic,
      final SequenceNumberFutureStore sequenceNumberFutureStore,
      final Map<String, Object> kafkaConsumerProperties,
      final Map<String, Object> kafkaProducerProperties,
      final Duration commandQueueCatchupTimeout
  ) {
    this.commandTopic = Objects.requireNonNull(commandTopic, "commandTopic");
    this.commandStatusMap = Maps.newConcurrentMap();
    this.sequenceNumberFutureStore =
        Objects.requireNonNull(sequenceNumberFutureStore, "sequenceNumberFutureStore");
    this.commandQueueCatchupTimeout =
        Objects.requireNonNull(commandQueueCatchupTimeout, "commandQueueCatchupTimeout");
    this.kafkaConsumerProperties =
        Objects.requireNonNull(kafkaConsumerProperties, "kafkaConsumerProperties");
    this.kafkaProducerProperties =
        Objects.requireNonNull(kafkaProducerProperties, "kafkaProducerProperties");
    this.commandTopicName = Objects.requireNonNull(commandTopicName, "commandTopicName");
  }

  @Override
  public void wakeup() {
    commandTopic.wakeup();
  }

  public String getCommandTopicName() {
    return commandTopic.getCommandTopicName();
  }

  public void start() {
    commandTopic.start();
  }

  /**
   * Close the store, rendering it unable to read commands
   */
  @Override
  public void close() {
    commandTopic.close();
  }

  @Override
  public QueuedCommandStatus enqueueCommand(
      final CommandId commandId,
      final Command command,
      final Producer<CommandId, Command> transactionalProducer
  ) {
    final CommandStatusFuture statusFuture = commandStatusMap.compute(
        commandId,
        (k, v) -> {
          if (v == null) {
            return new CommandStatusFuture(commandId);
          }
          // We should fail registration if a future is already registered, to prevent
          // a caller from receiving a future for a different statement.
          throw new IllegalStateException(
              String.format(
                  "Another command with the same id (%s) is being executed.",
                  commandId)
          );
        }
    );
    try {
      final ProducerRecord<CommandId, Command> producerRecord = new ProducerRecord<>(
          commandTopicName,
          COMMAND_TOPIC_PARTITION,
          commandId,
          command);
      final RecordMetadata recordMetadata =
          transactionalProducer.send(producerRecord).get();
      return new QueuedCommandStatus(recordMetadata.offset(), statusFuture);
    } catch (final Exception e) {
      commandStatusMap.remove(commandId);
      throw new KsqlStatementException(
          "Could not write the statement into the command topic.",
          String.format(
              "Could not write the statement '%s' into the command topic.",
              QueryMask.getMaskedStatement(command.getStatement())
          ),
          QueryMask.getMaskedStatement(command.getStatement()),
          KsqlStatementException.Problem.OTHER,
          e
      );
    }
  }

  @Override
  public List<QueuedCommand> getNewCommands(final Duration timeout) {
    completeSatisfiedSequenceNumberFutures();

    final List<QueuedCommand> queuedCommands = Lists.newArrayList();
    commandTopic.getNewCommands(timeout).forEach(
        c -> {
          if (c.value() != null) {
            queuedCommands.add(
                new QueuedCommand(
                    c.key(),
                    c.value(),
                    Optional.ofNullable(commandStatusMap.remove(c.key())),
                    c.offset()
                )
            );
          }
        }
    );
    return queuedCommands;
  }

  @Override
  public List<QueuedCommand> getRestoreCommands() {
    return commandTopic.getRestoreCommands(POLLING_TIMEOUT_FOR_COMMAND_TOPIC);
  }

  @Override
  public void ensureConsumedPast(final long seqNum, final Duration timeout)
      throws InterruptedException, TimeoutException {
    final CompletableFuture<Void> future =
        sequenceNumberFutureStore.getFutureForSequenceNumber(seqNum);
    try {
      future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(
          "Error waiting for command sequence number of " + seqNum, e.getCause());
    } catch (final TimeoutException e) {
      throw new TimeoutException(
          String.format(
              "Timeout reached while waiting for command sequence number of %d."
              + " Caused by: %s "
              + "(Timeout: %d ms)",
              seqNum,
              e.getMessage(),
              timeout.toMillis()
          ));
    }
  }

  @Override
  public Producer<CommandId, Command> createTransactionalProducer() {
    return new KafkaProducer<>(
        kafkaProducerProperties,
        InternalTopicSerdes.serializer(),
        InternalTopicSerdes.serializer()
    );
  }

  @Override
  public void waitForCommandConsumer() {
    try {
      final long endOffset = getCommandTopicOffset();
      ensureConsumedPast(endOffset - 1, commandQueueCatchupTimeout);
    } catch (final InterruptedException e) {
      final String errorMsg =
          "Interrupted while waiting for command topic consumer to process command topic";
      throw new KsqlServerException(errorMsg);
    } catch (final TimeoutException e) {
      final String errorMsg =
          "Timeout while waiting for command topic consumer to process command topic";
      throw new KsqlServerException(errorMsg);
    }
  }

  // Must create a new consumer because consumers are not threadsafe
  private long getCommandTopicOffset() {
    final TopicPartition commandTopicPartition = new TopicPartition(
        commandTopicName,
        COMMAND_TOPIC_PARTITION
    );

    try (Consumer<CommandId, Command> commandConsumer = new KafkaConsumer<>(
        kafkaConsumerProperties,
        InternalTopicSerdes.deserializer(CommandId.class),
        InternalTopicSerdes.deserializer(Command.class)
    )) {
      commandConsumer.assign(Collections.singleton(commandTopicPartition));
      return commandConsumer.endOffsets(Collections.singletonList(commandTopicPartition))
          .get(commandTopicPartition);
    }
  }

  @Override
  public boolean isEmpty() {
    return commandTopic.getEndOffset() == 0;
  }

  private void completeSatisfiedSequenceNumberFutures() {
    sequenceNumberFutureStore.completeFuturesUpToAndIncludingSequenceNumber(
        commandTopic.getCommandTopicConsumerPosition() - 1);
  }
}
