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
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.exception.ExceptionUtil;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.AlterSystemProperty;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.SpecificQueryIdGenerator;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.resources.KsqlConfigurable;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the actual execution (or delegation to KSQL core) of all distributed statements, as well
 * as tracking their statuses as things move along.
 */
public class InteractiveStatementExecutor implements KsqlConfigurable {

  private static final Logger log = LoggerFactory.getLogger(InteractiveStatementExecutor.class);

  private final ServiceContext serviceContext;
  private final KsqlEngine ksqlEngine;
  private final StatementParser statementParser;
  private final SpecificQueryIdGenerator queryIdGenerator;
  private final Map<CommandId, CommandStatus> statusStore;
  private final Deserializer<Command> commandDeserializer;
  private KsqlConfig ksqlConfig;

  private enum Mode {
    RESTORE,
    EXECUTE
  }

  public InteractiveStatementExecutor(
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final SpecificQueryIdGenerator queryIdGenerator
  ) {
    this(
        serviceContext,
        ksqlEngine,
        new StatementParser(ksqlEngine),
        queryIdGenerator,
        InternalTopicSerdes.deserializer(Command.class)
    );
  }

  @VisibleForTesting
  InteractiveStatementExecutor(
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final StatementParser statementParser,
      final SpecificQueryIdGenerator queryIdGenerator,
      final Deserializer<Command> commandDeserializer
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.statementParser = Objects.requireNonNull(statementParser, "statementParser");
    this.queryIdGenerator = Objects.requireNonNull(queryIdGenerator, "queryIdGenerator");
    this.commandDeserializer = Objects.requireNonNull(commandDeserializer, "commandDeserializer");
    this.statusStore = new ConcurrentHashMap<>();
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public void configure(final KsqlConfig config) {
    if (!config.getKsqlStreamConfigProps().containsKey(StreamsConfig.APPLICATION_SERVER_CONFIG)) {
      throw new IllegalArgumentException("Need KS application server set");
    }

    ksqlConfig = config;
  }

  KsqlEngine getKsqlEngine() {
    return ksqlEngine;
  }

  /**
   * Attempt to execute a single statement.
   *
   * @param queuedCommand The command to be executed
   */
  void handleStatement(final QueuedCommand queuedCommand) {
    throwIfNotConfigured();

    handleStatementWithTerminatedQueries(
        queuedCommand.getAndDeserializeCommand(commandDeserializer),
        queuedCommand.getAndDeserializeCommandId(),
        queuedCommand.getStatus(),
        Mode.EXECUTE,
        queuedCommand.getOffset(),
        false
    );
  }

  void handleRestore(final QueuedCommand queuedCommand) {
    throwIfNotConfigured();

    handleStatementWithTerminatedQueries(
        queuedCommand.getAndDeserializeCommand(commandDeserializer),
        queuedCommand.getAndDeserializeCommandId(),
        queuedCommand.getStatus(),
        Mode.RESTORE,
        queuedCommand.getOffset(),
        true
    );
  }

  /**
   * Get details on the statuses of all the statements handled thus far.
   *
   * @return A map detailing the current statuses of all statements that the handler has executed
   *     (or attempted to execute).
   */
  public Map<CommandId, CommandStatus> getStatuses() {
    return new HashMap<>(statusStore);
  }

  /**
   * @param statementId The ID of the statement to check the status of.
   * @return Information on the status of the statement with the given ID, if one exists.
   */
  public Optional<CommandStatus> getStatus(final CommandId statementId) {
    return Optional.ofNullable(statusStore.get(statementId));
  }

  private void throwIfNotConfigured() {
    if (ksqlConfig == null) {
      throw new IllegalStateException("No initialized");
    }
  }

  private void putStatus(final CommandId commandId,
                        final Optional<CommandStatusFuture> commandStatusFuture,
                        final CommandStatus status) {
    statusStore.put(commandId, status);
    commandStatusFuture.ifPresent(s -> s.setStatus(status));
  }

  private void putFinalStatus(final CommandId commandId,
                             final Optional<CommandStatusFuture> commandStatusFuture,
                             final CommandStatus status) {
    statusStore.put(commandId, status);
    commandStatusFuture.ifPresent(s -> s.setFinalStatus(status));
  }

  /**
   * Attempt to execute a single statement.
   *
   * @param command The string containing the statement to be executed
   * @param commandId The ID to be used to track the status of the command
   * @param mode was this table/stream subsequently dropped
   */
  private void handleStatementWithTerminatedQueries(
      final Command command,
      final CommandId commandId,
      final Optional<CommandStatusFuture> commandStatusFuture,
      final Mode mode,
      final long offset,
      final boolean restoreInProgress
  ) {
    try {
      if (command.getPlan().isPresent()) {
        executePlan(command, commandId, commandStatusFuture, command.getPlan().get(), mode, offset,
            restoreInProgress);
        return;
      }
      final String statementString = command.getStatement();
      putStatus(
          commandId,
          commandStatusFuture,
          new CommandStatus(CommandStatus.Status.PARSING, "Parsing statement"));
      final PreparedStatement<?> statement = statementParser.parseSingleStatement(statementString);
      putStatus(
          commandId,
          commandStatusFuture,
          new CommandStatus(CommandStatus.Status.EXECUTING, "Executing statement")
      );
      executeStatement(statement, commandId, commandStatusFuture);
    } catch (final KsqlException exception) {
      log.error("Failed to handle: " + command, exception);
      
      final CommandStatus errorStatus = new CommandStatus(
          CommandStatus.Status.ERROR,
          ExceptionUtil.stackTraceToString(exception)
      );
      putStatus(commandId, commandStatusFuture, errorStatus);
      throw exception;
    }
  }

  private void executePlan(
      final Command command,
      final CommandId commandId,
      final Optional<CommandStatusFuture> commandStatusFuture,
      final KsqlPlan plan,
      final Mode mode,
      final long offset,
      final boolean restoreInProgress
  ) {
    final KsqlConfig mergedConfig = buildMergedConfig(command);
    final ConfiguredKsqlPlan configured = ConfiguredKsqlPlan.of(
        plan,
        SessionConfig.of(mergedConfig, command.getOverwriteProperties())
    );
    putStatus(
        commandId,
        commandStatusFuture,
        new CommandStatus(CommandStatus.Status.EXECUTING, "Executing statement")
    );
    final ExecuteResult result = ksqlEngine.execute(serviceContext, configured,
        restoreInProgress);
    queryIdGenerator.setNextId(offset + 1);
    if (result.getQuery().isPresent()) {
      if (mode == Mode.EXECUTE) {
        result.getQuery().get().start();
      }
    }
    final String successMessage = getSuccessMessage(result);
    final Optional<QueryId> queryId = result.getQuery().map(QueryMetadata::getQueryId);
    final CommandStatus successStatus =
        new CommandStatus(CommandStatus.Status.SUCCESS, successMessage, queryId);
    putFinalStatus(commandId, commandStatusFuture, successStatus);
  }

  private String getSuccessMessage(final ExecuteResult result) {
    if (result.getCommandResult().isPresent()) {
      return result.getCommandResult().get();
    }
    return "Created query with ID " + (result.getQuery().get()).getQueryId();
  }

  @SuppressWarnings("unchecked")
  private void executeStatement(
      final PreparedStatement<?> statement,
      final CommandId commandId,
      final Optional<CommandStatusFuture> commandStatusFuture
  ) {
    if (statement.getStatement() instanceof TerminateQuery) {
      terminateQuery((PreparedStatement<TerminateQuery>) statement);

      final String successMessage = "Query terminated.";
      final CommandStatus successStatus =
          new CommandStatus(CommandStatus.Status.SUCCESS, successMessage, Optional.empty());

      putFinalStatus(commandId, commandStatusFuture, successStatus);
    } else if (statement.getStatement() instanceof ExecutableDdlStatement) {
      throwUnsupportedStatementError();
    } else if (statement.getStatement() instanceof CreateAsSelect) {
      throwUnsupportedStatementError();
    } else if (statement.getStatement() instanceof InsertInto) {
      throwUnsupportedStatementError();
    } else if (statement.getStatement() instanceof AlterSystemProperty) {
      final PreparedStatement<AlterSystemProperty> alterSystemQuery =
          (PreparedStatement<AlterSystemProperty>) statement;
      final String propertyName = alterSystemQuery.getStatement().getPropertyName();
      final String propertyValue = alterSystemQuery.getStatement().getPropertyValue();
      ksqlEngine.alterSystemProperty(propertyName, propertyValue);
      ksqlEngine.updateStreamsPropertiesAndRestartRuntime();

      final String successMessage = String.format("System property %s was set to %s.",
          propertyName, propertyValue);
      final CommandStatus successStatus = new CommandStatus(CommandStatus.Status.SUCCESS,
          successMessage, Optional.empty());

      putFinalStatus(commandId, commandStatusFuture, successStatus);
    } else {
      throw new KsqlException(String.format(
          "Unexpected statement type: %s",
          statement.getClass().getName()
      ));
    }
  }

  private KsqlConfig buildMergedConfig(final Command command) {
    return ksqlConfig.overrideBreakingConfigsWithOriginalValues(command.getOriginalProperties());
  }

  private void terminateQuery(final PreparedStatement<TerminateQuery> terminateQuery) {
    final Optional<QueryId> queryId = terminateQuery.getStatement().getQueryId();

    if (!queryId.isPresent()) {
      ksqlEngine.getPersistentQueries().forEach(PersistentQueryMetadata::close);
      return;
    }

    final Optional<PersistentQueryMetadata> query = ksqlEngine.getPersistentQuery(queryId.get());
    query.ifPresent(PersistentQueryMetadata::close);
  }

  private static void throwUnsupportedStatementError() {
    throw new KsqlException("This version of ksqlDB does not support executing "
        + "statements submitted prior to ksqlDB 0.8.0 or Confluent Platform ksqlDB 5.5. "
        + "Please see the upgrading guide to upgrade.");
  }

}
