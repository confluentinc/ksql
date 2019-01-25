/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.RetryUtil;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
  private final KsqlEngine ksqlEngine;
  private final CommandQueue commandStore;
  private volatile boolean closed;
  private final int maxRetries;
  private final ClusterTerminator clusterTerminator;

  public CommandRunner(
      final StatementExecutor statementExecutor,
      final CommandQueue commandStore,
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final int maxRetries
  ) {
    this(
        statementExecutor,
        commandStore,
        ksqlEngine,
        maxRetries,
        new ClusterTerminator(ksqlConfig, ksqlEngine)
    );
  }

  CommandRunner(
      final StatementExecutor statementExecutor,
      final CommandQueue commandStore,
      final KsqlEngine ksqlEngine,
      final int maxRetries,
      final ClusterTerminator clusterTerminator
  ) {
    this.statementExecutor = Objects.requireNonNull(statementExecutor, "statementExecutor");
    this.commandStore = Objects.requireNonNull(commandStore, "commandStore");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.maxRetries = maxRetries;
    closed = false;
    this.clusterTerminator = Objects.requireNonNull(clusterTerminator, "clusterTerminator");
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
    final Optional<QueuedCommand> terminateCmd = findTerminateCommand(commands);
    if (terminateCmd.isPresent()) {
      terminateCluster(terminateCmd.get().getCommand());
      return;
    }
    log.trace("Found {} new writes to command topic", commands.size());
    commands.forEach(this::executeStatement);
  }

  /**
   * Read and execute all commands on the command topic, starting at the earliest offset.
   */
  public void processPriorCommands() {
    final List<QueuedCommand> restoreCommands = commandStore.getRestoreCommands();
    final Optional<QueuedCommand> terminateCmd = findTerminateCommand(restoreCommands);
    if (terminateCmd.isPresent()) {
      terminateCluster(terminateCmd.get().getCommand());
      return;
    }
    restoreCommands.forEach(
        command -> RetryUtil.retryWithBackoff(
            maxRetries,
            STATEMENT_RETRY_MS,
            MAX_STATEMENT_RETRY_MS,
            () -> statementExecutor.handleRestore(command),
            WakeupException.class
        )
    );
    final KsqlEngine ksqlEngine = statementExecutor.getKsqlEngine();
    ksqlEngine.getPersistentQueries().forEach(PersistentQueryMetadata::start);
  }

  private void executeStatement(final QueuedCommand queuedCommand) {
    log.info("Executing statement: " + queuedCommand.getCommand().getStatement());
    RetryUtil.retryWithBackoff(
        maxRetries,
        STATEMENT_RETRY_MS,
        MAX_STATEMENT_RETRY_MS,
        () -> statementExecutor.handleStatement(queuedCommand),
        WakeupException.class
    );
  }

  private static Optional<QueuedCommand> findTerminateCommand(
      final List<QueuedCommand> restoreCommands
  ) {
    return restoreCommands.stream()
        .filter(command -> command.getCommand().getStatement()
            .equalsIgnoreCase(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT))
        .findFirst();
  }

  @SuppressWarnings("unchecked")
  private void terminateCluster(final Command command) {
    ksqlEngine.stopAcceptingStatements();
    log.info("Terminating the KSQL server.");
    this.close();
    final List<String> deleteTopicList = (List<String>) command.getOverwriteProperties()
        .getOrDefault(ClusterTerminateRequest.DELETE_TOPIC_LIST_PROP, Collections.emptyList());

    clusterTerminator.terminateCluster(deleteTopicList);
    log.info("The KSQL server was terminated.");
  }

}