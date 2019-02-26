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
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.rest.server.CommandTopic;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Wrapper class for the command topic. Used for reading from the topic (either all messages from
 * the beginning until now, or any new messages since then), and writing to it.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class CommandStore implements CommandQueue, Closeable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Duration POLLING_TIMEOUT_FOR_COMMAND_TOPIC = Duration.ofMillis(5000);

  private final CommandTopic commandTopic;
  private final CommandIdAssigner commandIdAssigner;
  private final Map<CommandId, CommandStatusFuture> commandStatusMap;
  private final SequenceNumberFutureStore sequenceNumberFutureStore;

  public CommandStore(
      final String commandTopicName,
      final Map<String, Object> kafkaConsumerProperties,
      final Map<String, Object> kafkaProducerProperties,
      final CommandIdAssigner commandIdAssigner
  ) {
    this(
        new CommandTopic(commandTopicName, kafkaConsumerProperties, kafkaProducerProperties),
        commandIdAssigner,
        new SequenceNumberFutureStore());
  }

  CommandStore(
      final CommandTopic commandTopic,
      final CommandIdAssigner commandIdAssigner,
      final SequenceNumberFutureStore sequenceNumberFutureStore
  ) {
    this.commandTopic = Objects.requireNonNull(commandTopic, "commandTopic");
    this.commandIdAssigner = Objects.requireNonNull(commandIdAssigner, "commandIdAssigner");
    this.commandStatusMap = Maps.newConcurrentMap();
    this.sequenceNumberFutureStore =
        Objects.requireNonNull(sequenceNumberFutureStore, "sequenceNumberFutureStore");
  }

  /**
   * Close the store, rendering it unable to read or write commands
   */
  @Override
  public void close() {
    commandTopic.close();
  }

  /**
   * Write the given statement to the command topic, to be read by all nodes in the current
   * cluster.
   * Does not return until the statement has been successfully written, or an exception is thrown.
   *
   * @param statement The statement to be distributed
   * @param overwriteProperties Any command-specific Streams properties to use.
   * @return The status of the enqueued command
   */
  @Override
  public QueuedCommandStatus enqueueCommand(
      final PreparedStatement<?> statement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overwriteProperties) {
    final CommandId commandId = commandIdAssigner.getCommandId(statement.getStatement());
    final Command command = new Command(
        statement.getStatementText(),
        overwriteProperties,
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandStatusFuture statusFuture = this.commandStatusMap.compute(
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
          commandTopic.send(commandId, command);
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

  /**
   * Poll for new commands, blocking until at least one is available.
   *
   * @return The commands that have been polled from the command topic
   */
  public List<QueuedCommand> getNewCommands() {
    completeSatisfiedSequenceNumberFutures();

    final List<QueuedCommand> queuedCommands = Lists.newArrayList();
    commandTopic.getNewCommands(Duration.ofMillis(Long.MAX_VALUE)).forEach(
        c -> {
          if (c.value() != null) {
            queuedCommands.add(
                new QueuedCommand(
                    c.key(),
                    c.value(),
                    Optional.ofNullable(commandStatusMap.remove(c.key()))
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
              "Timeout reached while waiting for command sequence number of %d. (Timeout: %d ms)",
              seqNum,
              timeout.toMillis()));
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