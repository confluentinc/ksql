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
import io.confluent.ksql.rest.server.TransactionalProducer;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;

/**
 * Wrapper class for the command topic. Used for reading from the topic (either all messages from
 * the beginning until now, or any new messages since then), and writing to it.
 */
public class CommandStore implements CommandQueue, Closeable {

  private static final Duration POLLING_TIMEOUT_FOR_COMMAND_TOPIC = Duration.ofMillis(5000);

  private final CommandTopic commandTopic;
  private final CommandIdAssigner commandIdAssigner;
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
          new CommandIdAssigner(),
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
      final CommandIdAssigner commandIdAssigner,
      final SequenceNumberFutureStore sequenceNumberFutureStore,
      final Map<String, Object> kafkaConsumerProperties,
      final Map<String, Object> kafkaProducerProperties,
      final Duration commandQueueCatchupTimeout
  ) {
    this.commandTopic = Objects.requireNonNull(commandTopic, "commandTopic");
    this.commandIdAssigner = Objects.requireNonNull(commandIdAssigner, "commandIdAssigner");
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
   * Close the store, rendering it unable to read or write commands
   */
  @Override
  public void close() {
    commandTopic.close();
  }

  @Override
  public TransactionalProducer createTransactionalProducer() {
    return new TransactionalProducer(
        commandTopicName,
        this,
        commandQueueCatchupTimeout,
        kafkaConsumerProperties,
        kafkaProducerProperties
    );
  }

  @Override
  public QueuedCommandStatus enqueueCommand(
      final ConfiguredStatement<?> statement,
      final TransactionalProducer transactionalProducer
  ) {
    final CommandId commandId = commandIdAssigner.getCommandId(statement.getStatement());

    // new commands that generate queries will use the new query id generation method from now on
    final Command command = new Command(
        statement.getStatementText(),
        true,
        statement.getOverrides(),
        statement.getConfig().getAllConfigPropsWithSecretsObfuscated()
    );

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
      final RecordMetadata recordMetadata =
          transactionalProducer.sendRecord(commandId, command);
      return new QueuedCommandStatus(recordMetadata.offset(), statusFuture);
    } catch (final Exception e) {
      commandStatusMap.remove(commandId);
      throw new KsqlException(
          String.format(
              "Could not write the statement '%s' into the "
                  + "command topic"
                  + ".", statement.getStatementText()
          ),
          e
      );
    }
  }

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

  public boolean isEmpty() {
    return commandTopic.getEndOffset() == 0;
  }

  private void completeSatisfiedSequenceNumberFutures() {
    sequenceNumberFutureStore.completeFuturesUpToAndIncludingSequenceNumber(
        commandTopic.getCommandTopicConsumerPosition() - 1);
  }
}
