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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.RetryUtil;
import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the logic of reading distributed commands, including pre-existing commands that were
 * issued before being initialized, and then delegating their execution to a
 * {@link InteractiveStatementExecutor}. 
 * Also responsible for taking care of any exceptions that occur in the process.
 */
public class CommandRunner implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(CommandRunner.class);

  private static final int STATEMENT_RETRY_MS = 100;
  private static final int MAX_STATEMENT_RETRY_MS = 5 * 1000;
  private static final Duration NEW_CMDS_TIMEOUT = Duration.ofMillis(MAX_STATEMENT_RETRY_MS);
  private static final int SHUTDOWN_TIMEOUT_MS = 3 * MAX_STATEMENT_RETRY_MS;

  private final InteractiveStatementExecutor statementExecutor;
  private final CommandQueue commandStore;
  private final ExecutorService executor;
  private volatile boolean closed = false;
  private final int maxRetries;
  private final ClusterTerminator clusterTerminator;
  private final ServerState serverState;

  private final CommandRunnerStatusMetric commandRunnerStatusMetric;
  private final AtomicReference<Pair<QueuedCommand, Instant>> currentCommandRef;
  private final Duration commandRunnerHealthTimeout;

  public enum CommandRunnerStatus {
    RUNNING,
    ERROR
  }

  public CommandRunner(
      final InteractiveStatementExecutor statementExecutor,
      final CommandQueue commandStore,
      final int maxRetries,
      final ClusterTerminator clusterTerminator,
      final ServerState serverState,
      final String ksqlServiceId,
      final Duration commandRunnerHealthTimeout,
      final String metricsGroupPrefix
  ) {
    this(
        statementExecutor,
        commandStore,
        maxRetries,
        clusterTerminator,
        Executors.newSingleThreadExecutor(r -> new Thread(r, "CommandRunner")),
        serverState,
        ksqlServiceId,
        commandRunnerHealthTimeout,
        metricsGroupPrefix
    );
  }

  @VisibleForTesting
  CommandRunner(
      final InteractiveStatementExecutor statementExecutor,
      final CommandQueue commandStore,
      final int maxRetries,
      final ClusterTerminator clusterTerminator,
      final ExecutorService executor,
      final ServerState serverState,
      final String ksqlServiceId,
      final Duration commandRunnerHealthTimeout,
      final String metricsGroupPrefix
  ) {
    this.statementExecutor = Objects.requireNonNull(statementExecutor, "statementExecutor");
    this.commandStore = Objects.requireNonNull(commandStore, "commandStore");
    this.maxRetries = maxRetries;
    this.clusterTerminator = Objects.requireNonNull(clusterTerminator, "clusterTerminator");
    this.executor = Objects.requireNonNull(executor, "executor");
    this.serverState = Objects.requireNonNull(serverState, "serverState");
    this.commandRunnerHealthTimeout =
        Objects.requireNonNull(commandRunnerHealthTimeout, "commandRunnerHealthTimeout");
    this.currentCommandRef = new AtomicReference<>(null);
    this.commandRunnerStatusMetric =
        new CommandRunnerStatusMetric(ksqlServiceId, this, metricsGroupPrefix);
  }

  /**
   * Begin a continuous poll-execute loop for the command topic, stopping only if either a
   * {@link WakeupException} is thrown or the {@link #close()} method is called.
   */
  public void start() {
    executor.execute(new Runner());
    executor.shutdown();
  }

  /**
   * Halt the poll-execute loop.
   */
  @Override
  public void close() {
    try {
      closed = true;
      commandStore.wakeup();
      executor.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    commandRunnerStatusMetric.close();
    commandStore.close();
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
        command -> {
          currentCommandRef.set(new Pair<>(command, Instant.now()));
          RetryUtil.retryWithBackoff(
              maxRetries,
              STATEMENT_RETRY_MS,
              MAX_STATEMENT_RETRY_MS,
              () -> statementExecutor.handleRestore(command),
              WakeupException.class
          );
          currentCommandRef.set(null);
        }
    );
    final KsqlEngine ksqlEngine = statementExecutor.getKsqlEngine();
    ksqlEngine.getPersistentQueries().forEach(PersistentQueryMetadata::start);
  }

  void fetchAndRunCommands() {
    final List<QueuedCommand> commands = commandStore.getNewCommands(NEW_CMDS_TIMEOUT);
    if (commands.isEmpty()) {
      return;
    }

    final Optional<QueuedCommand> terminateCmd = findTerminateCommand(commands);
    if (terminateCmd.isPresent()) {
      terminateCluster(terminateCmd.get().getCommand());
      return;
    }

    log.trace("Found {} new writes to command topic", commands.size());
    for (final QueuedCommand command : commands) {
      if (closed) {
        return;
      }

      executeStatement(command);
    }
  }

  private void executeStatement(final QueuedCommand queuedCommand) {
    log.info("Executing statement: " + queuedCommand.getCommand().getStatement());

    final Runnable task = () -> {
      if (closed) {
        log.info("Execution aborted as system is closing down");
      } else {
        statementExecutor.handleStatement(queuedCommand);
        log.info("Executed statement: " + queuedCommand.getCommand().getStatement());
      }
    };

    currentCommandRef.set(new Pair<>(queuedCommand, Instant.now()));
    RetryUtil.retryWithBackoff(
        maxRetries,
        STATEMENT_RETRY_MS,
        MAX_STATEMENT_RETRY_MS,
        task,
        WakeupException.class
    );
    currentCommandRef.set(null);
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
    serverState.setTerminating();
    log.info("Terminating the KSQL server.");
    this.close();
    final List<String> deleteTopicList = (List<String>) command.getOverwriteProperties()
        .getOrDefault(ClusterTerminateRequest.DELETE_TOPIC_LIST_PROP, Collections.emptyList());

    clusterTerminator.terminateCluster(deleteTopicList);
    log.info("The KSQL server was terminated.");
  }

  CommandRunnerStatus checkCommandRunnerStatus() {
    final Pair<QueuedCommand, Instant> currentCommand = currentCommandRef.get();
    if (currentCommand == null) {
      return CommandRunnerStatus.RUNNING;
    }

    return Duration.between(currentCommand.right, Instant.now()).toMillis()
        < commandRunnerHealthTimeout.toMillis()
        ? CommandRunnerStatus.RUNNING : CommandRunnerStatus.ERROR;
  }

  private class Runner implements Runnable {

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
  }
}
