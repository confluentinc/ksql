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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.server.resources.IncompatibleKsqlCommandVersionException;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.server.state.ServerState.State;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.PersistentQueryCleanupImpl;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
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
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
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
  private static final int COMMAND_TOPIC_THRESHOLD_LIMIT = 10000;

  private final InteractiveStatementExecutor statementExecutor;
  private final CommandQueue commandStore;
  private final ExecutorService executor;
  private final Function<List<QueuedCommand>, List<QueuedCommand>> compactor;
  private volatile boolean closed = false;
  private final int maxRetries;
  private final ClusterTerminator clusterTerminator;
  private final ServerState serverState;

  private final CommandRunnerMetrics commandRunnerMetric;
  private final AtomicReference<Pair<QueuedCommand, Instant>> currentCommandRef;
  private final AtomicReference<Instant> lastPollTime;
  private final Duration commandRunnerHealthTimeout;
  private final Clock clock;

  private final Deserializer<Command> commandDeserializer;
  private final Consumer<QueuedCommand> incompatibleCommandChecker;
  private final Errors errorHandler;
  private boolean incompatibleCommandDetected;
  private final Supplier<Boolean> commandTopicExists;
  private boolean commandTopicDeleted;
  private Status state = new Status(CommandRunnerStatus.RUNNING, CommandRunnerDegradedReason.NONE);

  /**
   * The ordinal values of the CommandRunnerStatus enum are used as the metrics values.
   * Please ensure preservation of the current order.
   */
  public enum CommandRunnerStatus {
    RUNNING,
    ERROR,
    DEGRADED
  }

  /**
   * The ordinal values of the CommandRunnerDegradedReason enum are used as the metrics values.
   * Please ensure preservation of the current order.
   */
  public enum CommandRunnerDegradedReason {
    NONE(errors -> ""),
    CORRUPTED(Errors::commandRunnerDegradedCorruptedErrorMessage),
    INCOMPATIBLE_COMMAND(Errors::commandRunnerDegradedIncompatibleCommandsErrorMessage);

    private final Function<Errors, String> msgFactory;

    public String getMsg(final Errors errors) {
      return msgFactory.apply(errors);
    }

    CommandRunnerDegradedReason(final Function<Errors, String> msgFactory) {
      this.msgFactory = msgFactory;
    }
  }

  public static class Status {
    private final CommandRunnerStatus status;
    private final CommandRunnerDegradedReason degradedReason;

    public Status(
        final CommandRunnerStatus status,
        final CommandRunnerDegradedReason degradedReason
    ) {
      this.status = status;
      this.degradedReason = degradedReason;
    }

    public CommandRunnerStatus getStatus() {
      return status;
    }

    public CommandRunnerDegradedReason getDegradedReason() {
      return degradedReason;
    }
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public CommandRunner(
      final InteractiveStatementExecutor statementExecutor,
      final CommandQueue commandStore,
      final int maxRetries,
      final ClusterTerminator clusterTerminator,
      final ServerState serverState,
      final String ksqlServiceId,
      final Duration commandRunnerHealthTimeout,
      final String metricsGroupPrefix,
      final Deserializer<Command> commandDeserializer,
      final Errors errorHandler,
      final KafkaTopicClient kafkaTopicClient,
      final String commandTopicName,
      final Metrics metrics
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
        commandDeserializer,
        errorHandler,
        () -> kafkaTopicClient.isTopicExists(commandTopicName),
        metrics
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
      final Deserializer<Command> commandDeserializer,
      final Errors errorHandler,
      final Supplier<Boolean> commandTopicExists,
      final Metrics metrics
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
    this.commandRunnerMetric =
        new CommandRunnerMetrics(ksqlServiceId, this, metricsGroupPrefix, metrics);
    this.clock = Objects.requireNonNull(clock, "clock");
    this.compactor = Objects.requireNonNull(compactor, "compactor");
    this.incompatibleCommandChecker =
        Objects.requireNonNull(incompatibleCommandChecker, "incompatibleCommandChecker");
    this.commandDeserializer =
        Objects.requireNonNull(commandDeserializer, "commandDeserializer");
    this.errorHandler =
        Objects.requireNonNull(errorHandler, "errorHandler");
    this.commandTopicExists =
        Objects.requireNonNull(commandTopicExists, "commandTopicExists");
    this.incompatibleCommandDetected = false;
    this.commandTopicDeleted = false;
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
    commandRunnerMetric.close();
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
  public void processPriorCommands(final PersistentQueryCleanupImpl queryCleanup) {
    try {
      final List<QueuedCommand> restoreCommands = commandStore.getRestoreCommands();
      final List<QueuedCommand> compatibleCommands = checkForIncompatibleCommands(restoreCommands);

      LOG.info("Restoring previous state from {} commands.", compatibleCommands.size());
      if (compatibleCommands.size() > COMMAND_TOPIC_THRESHOLD_LIMIT) {
        LOG.warn("Command topic size exceeded {} commands.", COMMAND_TOPIC_THRESHOLD_LIMIT);
      }

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

      if (commandStore.corruptionDetected()) {
        LOG.info("Corruption detected, queries will not be started.");
        queries.forEach(QueryMetadata::setCorruptionQueryError);
      } else {
        LOG.info("Restarting {} queries.", queries.size());
        queries.forEach(PersistentQueryMetadata::start);
        queryCleanup.cleanupLeakedQueries(queries);
        //We only want to clean up if the queries are read properly
        //We do not want to clean up potentially important stuff
        //when the cluster is in a bad state
      }

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
      if (!commandTopicExists.get()) {
        commandTopicDeleted = true;
      }
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
    final String commandId = queuedCommand.getAndDeserializeCommandId().toString();
    LOG.info("Executing statement: " + commandId);

    final Runnable task = () -> {
      if (closed) {
        LOG.info("Execution aborted as system is closing down");
      } else {
        statementExecutor.handleStatement(queuedCommand);
        LOG.info("Executed statement: " + commandId);
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
    serverState.setTerminated();
    LOG.info("The KSQL server was terminated.");
    closeEarly();
    LOG.debug("The KSQL command runner was closed.");
  }

  private List<QueuedCommand> checkForIncompatibleCommands(final List<QueuedCommand> commands) {
    final List<QueuedCommand> compatibleCommands = new ArrayList<>();
    try {
      for (final QueuedCommand command : commands) {
        incompatibleCommandChecker.accept(command);
        compatibleCommands.add(command);
      }
    } catch (final SerializationException | IncompatibleKsqlCommandVersionException e) {
      LOG.info("Incompatible command record detected when processing command topic", e);
      incompatibleCommandDetected = true;
    }
    return compatibleCommands;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "should be mutable")
  public CommandQueue getCommandQueue() {
    return commandStore;
  }

  public CommandRunnerStatus checkCommandRunnerStatus() {
    if (state.getStatus() == CommandRunnerStatus.DEGRADED) {
      return CommandRunnerStatus.DEGRADED;
    }

    final Pair<QueuedCommand, Instant> currentCommand = currentCommandRef.get();
    if (currentCommand == null) {
      state = lastPollTime.get() == null
          || Duration.between(lastPollTime.get(), clock.instant()).toMillis()
              < NEW_CMDS_TIMEOUT.toMillis() * 3
              ? new Status(CommandRunnerStatus.RUNNING, CommandRunnerDegradedReason.NONE)
                  : new Status(CommandRunnerStatus.ERROR, CommandRunnerDegradedReason.NONE);
      
    } else {
      state = Duration.between(currentCommand.right, clock.instant()).toMillis()
        < commandRunnerHealthTimeout.toMillis()
        ? new Status(CommandRunnerStatus.RUNNING, CommandRunnerDegradedReason.NONE)
              : new Status(CommandRunnerStatus.ERROR, CommandRunnerDegradedReason.NONE);
    }

    return state.getStatus();
  }

  public State checkServerState() {
    return this.serverState.getState();
  }

  public CommandRunnerDegradedReason getCommandRunnerDegradedReason() {
    return state.getDegradedReason();
  }

  public String getCommandRunnerDegradedWarning() {
    return getCommandRunnerDegradedReason().getMsg(errorHandler);
  }

  private class Runner implements Runnable {

    @Override
    public void run() {
      try {
        while (!closed) {
          if (incompatibleCommandDetected) {
            LOG.warn("CommandRunner entering degraded state due to "
                + "encountering an incompatible command");
            state = new Status(
                CommandRunnerStatus.DEGRADED,
                CommandRunnerDegradedReason.INCOMPATIBLE_COMMAND
            );
            closeEarly();
          } else if (commandStore.corruptionDetected()) {
            LOG.warn("CommandRunner entering degraded state due to encountering corruption "
                + "between topic and backup");
            state = new Status(
                CommandRunnerStatus.DEGRADED,
                CommandRunnerDegradedReason.CORRUPTED
            );
            closeEarly();
          } else if (commandTopicDeleted) {
            LOG.warn("CommandRunner entering degraded state due to command topic deletion.");
            state = new Status(
                CommandRunnerStatus.DEGRADED,
                CommandRunnerDegradedReason.CORRUPTED
            );
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
      } catch (final OffsetOutOfRangeException e) {
        LOG.warn("The command topic offset was reset. CommandRunner thread exiting.");
        state = new Status(
            CommandRunnerStatus.DEGRADED,
            CommandRunnerDegradedReason.CORRUPTED
        );
        closeEarly();
      } finally {
        LOG.info("Closing command store");
        commandStore.close();
      }
    }
  }
}
