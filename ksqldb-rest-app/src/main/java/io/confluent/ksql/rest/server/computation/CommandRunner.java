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
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.RetryUtil;
import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the logic of reading distributed commands, including pre-existing commands that were
 * issued before being initialized, and then delegating their execution to a {@link
 * InteractiveStatementExecutor}. Also responsible for taking care of any exceptions that occur in
 * the process.
 */
public class CommandRunner implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(CommandRunner.class);

  private static final int STATEMENT_RETRY_MS = 100;
  private static final int MAX_STATEMENT_RETRY_MS = 5 * 1000;
  private static final Duration NEW_CMDS_TIMEOUT = Duration.ofMillis(MAX_STATEMENT_RETRY_MS);
  private static final int SHUTDOWN_TIMEOUT_MS = 3 * MAX_STATEMENT_RETRY_MS;

  private final InteractiveStatementExecutor statementExecutor;
  private final CommandQueue commandStore;
  private final ExecutorService executor;
  private final Function<List<QueuedCommand>, List<QueuedCommand>> compactor;
  private volatile boolean closed = false;
  private final int maxRetries;
  private final ClusterTerminator clusterTerminator;
  private final ServerState serverState;

  private final CommandRunnerStatusMetric commandRunnerStatusMetric;
  private final AtomicReference<Pair<QueuedCommand, Instant>> currentCommandRef;
  private final AtomicReference<Instant> lastPollTime;
  private final Duration commandRunnerHealthTimeout;
  private final Clock clock;

  private final Deserializer<Command> commandDeserializer;
  private final Consumer<QueuedCommand> incompatibleCommandChecker;
  private boolean deserializationErrorThrown;

  public enum CommandRunnerStatus {
    RUNNING,
    ERROR,
    DEGRADED
  }

  public CommandRunner(
      final InteractiveStatementExecutor statementExecutor,
      final CommandQueue commandStore,
      final int maxRetries,
      final ClusterTerminator clusterTerminator,
      final ServerState serverState,
      final String ksqlServiceId,
      final Duration commandRunnerHealthTimeout,
      final String metricsGroupPrefix,
      final Deserializer<Command> commandDeserializer
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
        metricsGroupPrefix,
        Clock.systemUTC(),
        RestoreCommandsCompactor::compact,
        queuedCommand -> {
          queuedCommand.getAndDeserializeCommandId();
          queuedCommand.getAndDeserializeCommand(commandDeserializer);
        },
        commandDeserializer
    );
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
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
      final String metricsGroupPrefix,
      final Clock clock,
      final Function<List<QueuedCommand>, List<QueuedCommand>> compactor,
      final Consumer<QueuedCommand> incompatibleCommandChecker,
      final Deserializer<Command> commandDeserializer
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.statementExecutor = Objects.requireNonNull(statementExecutor, "statementExecutor");
    this.commandStore = Objects.requireNonNull(commandStore, "commandStore");
    this.maxRetries = maxRetries;
    this.clusterTerminator = Objects.requireNonNull(clusterTerminator, "clusterTerminator");
    this.executor = Objects.requireNonNull(executor, "executor");
    this.serverState = Objects.requireNonNull(serverState, "serverState");
    this.commandRunnerHealthTimeout =
        Objects.requireNonNull(commandRunnerHealthTimeout, "commandRunnerHealthTimeout");
    this.currentCommandRef = new AtomicReference<>(null);
    this.lastPollTime = new AtomicReference<>(null);
    this.commandRunnerStatusMetric =
        new CommandRunnerStatusMetric(ksqlServiceId, this, metricsGroupPrefix);
    this.clock = Objects.requireNonNull(clock, "clock");
    this.compactor = Objects.requireNonNull(compactor, "compactor");
    this.incompatibleCommandChecker =
        Objects.requireNonNull(incompatibleCommandChecker, "incompatibleCommandChecker");
    this.commandDeserializer =
        Objects.requireNonNull(commandDeserializer, "commandDeserializer");
    this.deserializationErrorThrown = false;
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
    if (!closed) {
      closeEarly();
    }
    commandRunnerStatusMetric.close();
  }

  /**
   * Closes the poll-execute loop before the server shuts down
   */
  private void closeEarly() {
    try {
      closed = true;
      commandStore.wakeup();
      executor.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Read and execute all commands on the command topic, starting at the earliest offset.
   */
  public void processPriorCommands() {
    try {
      final List<QueuedCommand> restoreCommands = commandStore.getRestoreCommands();
      final List<QueuedCommand> compatibleCommands = checkForIncompatibleCommands(restoreCommands);

      LOG.info("Restoring previous state from {} commands.", compatibleCommands.size());

      final Optional<QueuedCommand> terminateCmd =
          findTerminateCommand(compatibleCommands, commandDeserializer);
      if (terminateCmd.isPresent()) {
        LOG.info("Cluster previously terminated: terminating.");
        terminateCluster(terminateCmd.get().getAndDeserializeCommand(commandDeserializer));
        return;
      }

      final List<QueuedCommand> compacted = compactor.apply(compatibleCommands);

      compacted.forEach(
          command -> {
            currentCommandRef.set(new Pair<>(command, clock.instant()));
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

      final List<PersistentQueryMetadata> queries = statementExecutor
          .getKsqlEngine()
          .getPersistentQueries();

      LOG.info("Restarting {} queries.", queries.size());

      queries.forEach(PersistentQueryMetadata::start);

      LOG.info("Restore complete");

    } catch (final Exception e) {
      LOG.error("Error during restore", e);
      throw e;
    }
  }

  void fetchAndRunCommands() {
    lastPollTime.set(clock.instant());
    final List<QueuedCommand> commands = commandStore.getNewCommands(NEW_CMDS_TIMEOUT);
    if (commands.isEmpty()) {
      return;
    }

    final List<QueuedCommand> compatibleCommands = checkForIncompatibleCommands(commands);
    final Optional<QueuedCommand> terminateCmd =
        findTerminateCommand(compatibleCommands, commandDeserializer);
    if (terminateCmd.isPresent()) {
      terminateCluster(terminateCmd.get().getAndDeserializeCommand(commandDeserializer));
      return;
    }

    LOG.debug("Found {} new writes to command topic", compatibleCommands.size());
    for (final QueuedCommand command : compatibleCommands) {
      if (closed) {
        return;
      }

      executeStatement(command);
    }
  }

  private void executeStatement(final QueuedCommand queuedCommand) {
    final String commandStatement =
        queuedCommand.getAndDeserializeCommand(commandDeserializer).getStatement();
    LOG.info("Executing statement: " + commandStatement);

    final Runnable task = () -> {
      if (closed) {
        LOG.info("Execution aborted as system is closing down");
      } else {
        statementExecutor.handleStatement(queuedCommand);
        LOG.info("Executed statement: " + commandStatement);
      }
    };

    currentCommandRef.set(new Pair<>(queuedCommand, clock.instant()));
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
      final List<QueuedCommand> restoreCommands,
      final Deserializer<Command> commandDeserializer
  ) {
    return restoreCommands.stream()
        .filter(command -> command.getAndDeserializeCommand(commandDeserializer).getStatement()
            .equalsIgnoreCase(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT))
        .findFirst();
  }

  @SuppressWarnings("unchecked")
  private void terminateCluster(final Command command) {
    serverState.setTerminating();
    LOG.info("Terminating the KSQL server.");
    this.close();
    final List<String> deleteTopicList = (List<String>) command.getOverwriteProperties()
        .getOrDefault(ClusterTerminateRequest.DELETE_TOPIC_LIST_PROP, Collections.emptyList());

    clusterTerminator.terminateCluster(deleteTopicList);
    LOG.info("The KSQL server was terminated.");
  }

  public CommandRunnerStatus checkCommandRunnerStatus() {
    if (deserializationErrorThrown) {
      return CommandRunnerStatus.DEGRADED;
    }

    final Pair<QueuedCommand, Instant> currentCommand = currentCommandRef.get();
    if (currentCommand == null) {
      return lastPollTime.get() == null
          || Duration.between(lastPollTime.get(), clock.instant()).toMillis()
              < NEW_CMDS_TIMEOUT.toMillis() * 3
              ? CommandRunnerStatus.RUNNING : CommandRunnerStatus.ERROR;
    }
    
    return Duration.between(currentCommand.right, clock.instant()).toMillis()
        < commandRunnerHealthTimeout.toMillis()
        ? CommandRunnerStatus.RUNNING : CommandRunnerStatus.ERROR;
  }

  private List<QueuedCommand> checkForIncompatibleCommands(final List<QueuedCommand> commands) {
    final List<QueuedCommand> compatibleCommands = new ArrayList<>();
    try {
      for (final QueuedCommand command : commands) {
        incompatibleCommandChecker.accept(command);
        compatibleCommands.add(command);
      }
    } catch (SerializationException e) {
      LOG.info("Deserialization error detected when processing record", e);
      deserializationErrorThrown = true;
    }
    return compatibleCommands;
  }

  public CommandQueue getCommandQueue() {
    return commandStore;
  }

  private class Runner implements Runnable {

    @Override
    public void run() {
      try {
        while (!closed) {
          if (deserializationErrorThrown) {
            LOG.warn("CommandRunner entering degraded state after failing to deserialize command");
            closeEarly();
          } else {
            LOG.trace("Polling for new writes to command topic");
            fetchAndRunCommands();
          }
        }
      } catch (final WakeupException wue) {
        if (!closed) {
          throw wue;
        }
      } finally {
        commandStore.close();
      }
    }
  }
}
