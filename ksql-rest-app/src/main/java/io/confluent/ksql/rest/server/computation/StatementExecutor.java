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

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.commands.DdlCommandResult;
import io.confluent.ksql.exception.ExceptionUtil;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the actual execution (or delegation to KSQL core) of all distributed statements, as well
 * as tracking their statuses as things move along.
 */
public class StatementExecutor {

  private static final Logger log = LoggerFactory.getLogger(StatementExecutor.class);

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final StatementParser statementParser;
  private final Map<CommandId, CommandStatus> statusStore;
  private final Map<CommandId, CommandStatusFuture> statusFutures;

  public StatementExecutor(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final StatementParser statementParser
  ) {
    this.ksqlConfig = ksqlConfig;
    this.ksqlEngine = ksqlEngine;
    this.statementParser = statementParser;

    this.statusStore = new HashMap<>();
    this.statusFutures = new HashMap<>();
  }

  void handleRestoration(final RestoreCommands restoreCommands) {
    restoreCommands.forEach(
        (commandId, command, terminatedQueries, wasDropped) -> {
          log.info("Executing prior statement: '{}'", command);
          try {
            handleStatementWithTerminatedQueries(
                command,
                commandId,
                terminatedQueries,
                wasDropped
            );
          } catch (final Exception exception) {
            log.warn(
                "Failed to execute statement due to exception",
                exception
            );
          }
        }
    );
  }

  /**
   * Attempt to execute a single statement.
   *
   * @param command The string containing the statement to be executed
   * @param commandId The ID to be used to track the status of the command
   */
  void handleStatement(
      final Command command,
      final CommandId commandId
  ) {
    handleStatementWithTerminatedQueries(command,
                                         commandId,
                                         null,
                                         false);
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

  /**
   * Register the existence of a new statement that has been written to the command topic. All other
   * statement status information is updated exclusively by the current {@link StatementExecutor}
   * instance, but in the (unlikely but possible) event that a statement is written to the command
   * topic but never picked up by this instance, it should be possible to know that it was at least
   * written to the topic in the first place.
   *
   * @param commandId The ID of the statement that has been written to the command topic.
   */
  public Future<CommandStatus> registerQueuedStatement(final CommandId commandId) {
    statusStore.put(
        commandId,
        new CommandStatus(CommandStatus.Status.QUEUED, "Statement written to command topic")
    );

    CommandStatusFuture result;
    synchronized (statusFutures) {
      result = statusFutures.get(commandId);
      if (result != null) {
        return result;
      } else {
        result = new CommandStatusFuture(commandId, statusFutures::remove);
        statusFutures.put(commandId, result);
        return result;
      }
    }
  }

  private void completeStatusFuture(final CommandId commandId, final CommandStatus commandStatus) {
    synchronized (statusFutures) {
      final CommandStatusFuture statusFuture = statusFutures.get(commandId);
      if (statusFuture != null) {
        statusFuture.complete(commandStatus);
      } else {
        final CommandStatusFuture newStatusFuture = new CommandStatusFuture(
            commandId,
            statusFutures::remove
        );
        newStatusFuture.complete(commandStatus);
        statusFutures.put(commandId, newStatusFuture);
      }
    }
  }

  /**
   * Attempt to execute a single statement.
   *
   * @param command The string containing the statement to be executed
   * @param commandId The ID to be used to track the status of the command
   * @param terminatedQueries An optional map from terminated query IDs to the commands that
   *     requested their termination
   * @param wasDropped was this table/stream subsequently dropped
   */
  private void handleStatementWithTerminatedQueries(
      final Command command,
      final CommandId commandId,
      final Map<QueryId, CommandId> terminatedQueries,
      final boolean wasDropped
  ) {
    try {
      final String statementString = command.getStatement();
      statusStore.put(
          commandId,
          new CommandStatus(CommandStatus.Status.PARSING, "Parsing statement")
      );
      final Statement statement = statementParser.parseSingleStatement(statementString);
      statusStore.put(
          commandId,
          new CommandStatus(CommandStatus.Status.EXECUTING, "Executing statement")
      );
      executeStatement(statement, command, commandId, terminatedQueries, wasDropped);
    } catch (final WakeupException exception) {
      throw exception;
    } catch (final Exception exception) {
      log.error("Failed to handle: " + command, exception);
      final CommandStatus errorStatus = new CommandStatus(
          CommandStatus.Status.ERROR,
          ExceptionUtil.stackTraceToString(exception)
      );
      statusStore.put(commandId, errorStatus);
      completeStatusFuture(commandId, errorStatus);
    }
  }

  private void executeStatement(
      final Statement statement,
      final Command command,
      final CommandId commandId,
      final Map<QueryId, CommandId> terminatedQueries,
      final boolean wasDropped
  ) throws Exception {
    final String statementStr = command.getStatement();

    DdlCommandResult result = null;
    String successMessage = "";
    if (statement instanceof DdlStatement) {
      result = ksqlEngine.executeDdlStatement(
          statementStr,
          (DdlStatement) statement,
          command.getOverwriteProperties());
    } else if (statement instanceof CreateAsSelect) {
      successMessage = handleCreateAsSelect(
          (CreateAsSelect)
              statement,
          command,
          commandId,
          terminatedQueries,
          statementStr,
          wasDropped);
      if (successMessage == null) {
        return;
      }
    } else if (statement instanceof InsertInto) {
      successMessage = handleInsertInto((InsertInto) statement,
                       command,
                       commandId,
                       terminatedQueries,
                       statementStr,
                       false
                       );
      if (successMessage == null) {
        return;
      }
    } else if (statement instanceof TerminateQuery) {
      terminateQuery((TerminateQuery) statement);
      successMessage = "Query terminated.";
    } else if (statement instanceof RunScript) {
      handleRunScript(command);
    } else {
      throw new Exception(String.format(
          "Unexpected statement type: %s",
          statement.getClass().getName()
      ));
    }
    // TODO: change to unified return message
    final CommandStatus successStatus = new CommandStatus(
        CommandStatus.Status.SUCCESS,
        result != null ? result.getMessage() : successMessage
    );
    statusStore.put(commandId, successStatus);
    completeStatusFuture(commandId, successStatus);
  }

  private void handleRunScript(final Command command) {

    if (command.getOverwriteProperties().containsKey(KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT)) {
      final String queries =
          (String) command.getOverwriteProperties().get(
              KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT);
      final Map<String, Object> overriddenProperties = new HashMap<>();
      overriddenProperties.putAll(command.getOverwriteProperties());

      final List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(
          queries,
          ksqlConfig.overrideBreakingConfigsWithOriginalValues(command.getOriginalProperties()),
          overriddenProperties
      );
      for (final QueryMetadata queryMetadata : queryMetadataList) {
        if (queryMetadata instanceof PersistentQueryMetadata) {
          final PersistentQueryMetadata persistentQueryMd = (PersistentQueryMetadata) queryMetadata;
          persistentQueryMd.getKafkaStreams().start();
        }
      }
    } else {
      throw new KsqlException("No statements received for LOAD FROM FILE.");
    }

  }

  private String handleCreateAsSelect(
      final CreateAsSelect statement,
      final Command command,
      final CommandId commandId,
      final Map<QueryId, CommandId> terminatedQueries,
      final String statementStr,
      final boolean wasDropped
  ) throws Exception {
    final QuerySpecification querySpecification =
        (QuerySpecification) statement.getQuery().getQueryBody();
    final Query query = ksqlEngine.addInto(
        statement.getQuery(),
        querySpecification,
        statement.getName().getSuffix(),
        statement.getProperties(),
        statement.getPartitionByColumn(),
        true
    );
    if (startQuery(statementStr, query, commandId, terminatedQueries, command, wasDropped)) {
      return statement instanceof CreateTableAsSelect
             ? "Table created and running"
             : "Stream created and running";
    }

    return null;
  }

  private String handleInsertInto(final InsertInto statement,
                                      final Command command,
                                      final CommandId commandId,
                                      final Map<QueryId, CommandId> terminatedQueries,
                                      final String statementStr,
                                      final boolean wasDropped) throws Exception {
    final QuerySpecification querySpecification =
        (QuerySpecification) statement.getQuery().getQueryBody();
    final Query query = ksqlEngine.addInto(
        statement.getQuery(),
        querySpecification,
        statement.getTarget().getSuffix(),
        new HashMap<>(),
        Optional.empty(),
        false
    );
    if (startQuery(statementStr, query, commandId, terminatedQueries, command, wasDropped)) {
      return "Insert Into query is running.";
    }

    return null;
  }

  private boolean startQuery(
      final String queryString,
      final Query query,
      final CommandId commandId,
      final Map<QueryId, CommandId> terminatedQueries,
      final Command command,
      final boolean wasDropped
  ) throws Exception {
    if (query.getQueryBody() instanceof QuerySpecification) {
      final QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
      final Relation into = querySpecification.getInto();
      if (into instanceof Table) {
        final Table table = (Table) into;
        if (ksqlEngine.getMetaStore().getSource(table.getName().getSuffix()) != null
            && querySpecification.isShouldCreateInto()) {
          throw new Exception(String.format(
              "Sink specified in INTO clause already exists: %s",
              table.getName().getSuffix().toUpperCase()
          ));
        }
      }
    }

    final QueryMetadata queryMetadata = ksqlEngine.buildMultipleQueries(
        queryString,
        ksqlConfig.overrideBreakingConfigsWithOriginalValues(command.getOriginalProperties()),
        command.getOverwriteProperties()
    ).get(0);

    if (queryMetadata instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQueryMd = (PersistentQueryMetadata) queryMetadata;
      final QueryId queryId = persistentQueryMd.getQueryId();

      if (terminatedQueries != null && terminatedQueries.containsKey(queryId)) {
        final CommandId terminateId = terminatedQueries.get(queryId);
        statusStore.put(
            terminateId,
            new CommandStatus(CommandStatus.Status.SUCCESS, "Termination request granted")
        );
        statusStore.put(
            commandId,
            new CommandStatus(CommandStatus.Status.TERMINATED, "Query terminated")
        );
        ksqlEngine.terminateQuery(queryId, false);
        return false;
      } else if (wasDropped) {
        ksqlEngine.terminateQuery(queryId, false);
        return false;
      } else {
        persistentQueryMd.getKafkaStreams().start();
        return true;
      }

    } else {
      throw new Exception(String.format(
          "Unexpected query metadata type: %s; was expecting %s",
          queryMetadata.getClass().getCanonicalName(),
          PersistentQueryMetadata.class.getCanonicalName()
      ));
    }
  }

  private void terminateQuery(final TerminateQuery terminateQuery) throws Exception {

    final QueryId queryId = terminateQuery.getQueryId();
    final QueryMetadata queryMetadata = ksqlEngine.getPersistentQuery(queryId);
    if (!ksqlEngine.terminateQuery(queryId, true)) {
      throw new Exception(String.format("No running query with id %s was found", queryId));
    }

    final CommandId.Type commandType;
    final DataSource.DataSourceType sourceType =
        queryMetadata.getOutputNode().getTheSourceNode().getDataSourceType();
    switch (sourceType) {
      case KTABLE:
        commandType = CommandId.Type.TABLE;
        break;
      case KSTREAM:
        commandType = CommandId.Type.STREAM;
        break;
      default:
        throw new
            Exception(String.format("Unexpected source type for running query: %s", sourceType));
    }

    final String queryEntity =
        ((KsqlStructuredDataOutputNode) queryMetadata.getOutputNode()).getKsqlTopic().getName();

    final CommandId queryStmtId = new CommandId(commandType, queryEntity, CommandId.Action.CREATE);
    statusStore.put(
        queryStmtId,
        new CommandStatus(CommandStatus.Status.TERMINATED, "Query terminated")
    );
  }
}
