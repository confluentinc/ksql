/*
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

package io.confluent.ksql.rest.server.computation;

import com.google.common.collect.Maps;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.server.CommandTopic;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for the command topic. Used for reading from the topic (either all messages from
 * the beginning until now, or any new messages since then), and writing to it.
 */

public class CommandStore implements ReplayableCommandQueue, Closeable {

  private static final Logger log = LoggerFactory.getLogger(CommandStore.class);

  private static final Duration POLLING_TIMEOUT_FOR_COMMAND_TOPIC = Duration.ofMillis(5000);

  private final CommandTopic commandTopic;
  private final CommandIdAssigner commandIdAssigner;
  private final Map<CommandId, QueuedCommandStatus> commandStatusMap;

  public CommandStore(
      final String commandTopicName,
      final Map<String, Object> commandConsumerProperties,
      final CommandIdAssigner commandIdAssigner
  ) {
    this(commandIdAssigner, new CommandTopic(commandTopicName, commandConsumerProperties));
  }

  CommandStore(
      final CommandIdAssigner commandIdAssigner,
      final CommandTopic commandTopic
  ) {
    Objects.requireNonNull(commandIdAssigner, "commandIdAssigner");
    Objects.requireNonNull(commandTopic, "commandTopic");
    this.commandIdAssigner = commandIdAssigner;
    this.commandStatusMap = Maps.newConcurrentMap();
    this.commandTopic = commandTopic;
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
   * @param statementString The string of the statement to be distributed
   * @param statement The statement to be distributed
   * @param overwriteProperties Any command-specific Streams properties to use.
   * @return The status of the enqueued command
   */
  @Override
  public QueuedCommandStatus enqueueCommand(
      final String statementString,
      final Statement statement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overwriteProperties) {
    final CommandId commandId = commandIdAssigner.getCommandId(statement);
    final Command command = new Command(
        statementString,
        overwriteProperties,
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final QueuedCommandStatus status = new QueuedCommandStatus(commandId);
    this.commandStatusMap.compute(
        commandId,
        (k, v) -> {
          if (v == null) {
            return status;
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
      commandTopic.send(commandId, command);
    } catch (final Exception e) {
      commandStatusMap.remove(commandId);
      throw new KsqlException(
          String.format(
              "Could not write the statement '%s' into the "
              + "command topic"
              + ".", statementString
          ),
          e
      );
    }
    return status;
  }

  /**
   * Poll for new commands, blocking until at least one is available.
   *
   * @return The commands that have been polled from the command topic
   */
  public List<QueuedCommand> getNewCommands() {
    return commandTopic.getNewCommands(commandStatusMap);
  }

  public RestoreCommands getRestoreCommands() {
    return commandTopic.getRestoreCommands(POLLING_TIMEOUT_FOR_COMMAND_TOPIC);
  }

}
