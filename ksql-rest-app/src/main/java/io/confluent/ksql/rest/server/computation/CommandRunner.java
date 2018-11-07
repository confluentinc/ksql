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

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.util.RetryUtil;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the logic of reading distributed commands, including pre-existing commands that were
 * issued before being initialized, and then delegating their execution to a
 * {@link StatementExecutor}. Also responsible for taking care of any exceptions that occur in the
 * process.
 */
public class CommandRunner implements Runnable, Closeable {

  private static final Logger log = LoggerFactory.getLogger(CommandRunner.class);

  private static final int STATEMENT_RETRY_MS = 100;
  private static final int MAX_STATEMENT_RETRY_MS = 5 * 1000;

  private final StatementExecutor statementExecutor;
  private final CommandStore commandStore;
  private volatile boolean closed;
  private final int maxRetries;

  public CommandRunner(
      final StatementExecutor statementExecutor,
      final CommandStore commandStore,
      final int maxRetries
  ) {
    this.statementExecutor = statementExecutor;
    this.commandStore = commandStore;
    this.maxRetries = maxRetries;
    closed = false;
  }

  /**
   * Begin a continuous poll-execute loop for the command topic, stopping only if either a
   * {@link WakeupException} is thrown or the {@link #close()} method is called.
   */
  @Override
  public void run() {
    try {
      while (!closed) {
        log.debug("Polling for new writes to command topic");
        fetchAndRunCommands();
      }
    } catch (final WakeupException wue) {
      if (!closed) {
        throw wue;
      }
    }
  }

  /**
   * Halt the poll-execute loop.
   */
  @Override
  public void close() {
    closed = true;
    commandStore.close();
  }

  void fetchAndRunCommands() {
    final List<QueuedCommand> commands = commandStore.getNewCommands();
    log.trace("Found {} new writes to command topic", commands.size());
    commands.stream()
        .filter(c -> c.getCommand().isPresent())
        .forEach(c -> executeStatement(c.getCommand().get(), c.getCommandId(), c.getStatus()));
  }

  /**
   * Read and execute all commands on the command topic, starting at the earliest offset.
   * @throws Exception TODO: Refine this.
   */
  public void processPriorCommands() {
    final RestoreCommands restoreCommands = commandStore.getRestoreCommands();
    restoreCommands.forEach(
        (commandId, command, terminatedQueries, wasDropped) -> {
          RetryUtil.retryWithBackoff(
              maxRetries,
              STATEMENT_RETRY_MS,
              MAX_STATEMENT_RETRY_MS,
              () -> statementExecutor.handleStatementWithTerminatedQueries(
                  command,
                  commandId,
                  Optional.empty(),
                  terminatedQueries,
                  wasDropped
              ),
              WakeupException.class
          );
        }
    );
  }

  private void executeStatement(final Command command,
                                final CommandId commandId,
                                final Optional<QueuedCommandStatus> status) {
    log.info("Executing statement: " + command.getStatement());
    RetryUtil.retryWithBackoff(
        maxRetries,
        STATEMENT_RETRY_MS,
        MAX_STATEMENT_RETRY_MS,
        () -> statementExecutor.handleStatement(command, commandId, status),
        WakeupException.class
    );
  }
}
