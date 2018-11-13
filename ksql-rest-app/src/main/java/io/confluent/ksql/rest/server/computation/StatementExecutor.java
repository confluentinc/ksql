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

import com.google.common.collect.Lists;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.commands.DdlCommandResult;
import io.confluent.ksql.exception.ExceptionUtil;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandId.Action;
import io.confluent.ksql.rest.server.computation.CommandId.Type;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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

  private enum Mode {
    RESTORE,
    EXECUTE
  }

  public StatementExecutor(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final StatementParser statementParser
  ) {
    this.ksqlConfig = ksqlConfig;
    this.ksqlEngine = ksqlEngine;
    this.statementParser = statementParser;

    this.statusStore = new ConcurrentHashMap<>();
  }

  void handleRestoration(final List<QueuedCommand> restoreCommands) {
    final List<QueryId> restoredQueries = Lists.newArrayList();
    restoreCommands.forEach(
        command -> {
          log.info("Executing prior statement: '{}'", command);
          try {
            handleStatementWithTerminatedQueries(
                command.getCommand(),
                command.getCommandId(),
                Optional.empty(),
                Mode.RESTORE,
                restoredQueries
            );
          } catch (final Exception exception) {
            log.warn(
                "Failed to execute statement due to exception",
                exception
            );
          }
        }
    );
    for (int i = 0; i < restoredQueries.size(); i++) {
      final QueryId queryId = restoredQueries.get(i);
      if (restoredQueries.lastIndexOf(queryId) > i) {
        continue;
      }
      if (ksqlEngine.getPersistentQuery(queryId) == null) {
        continue;
      }
      ksqlEngine.getPersistentQuery(queryId).start();
    }
  }

  /**
   * Attempt to execute a single statement.
   *
   * @param command The string containing the statement to be executed
   * @param commandId The ID to be used to track the status of the command
   */
  void handleStatement(
      final Command command,
      final CommandId commandId,
      final Optional<QueuedCommandStatus> status
  ) {
    handleStatementWithTerminatedQueries(command,
        commandId,
        status,
        Mode.EXECUTE,
        null);
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

  public void putStatus(final CommandId commandId,
                        final Optional<QueuedCommandStatus> queuedCommandStatus,
                        final CommandStatus status) {
    statusStore.put(commandId, status);
    queuedCommandStatus.ifPresent(s -> s.setStatus(status));
  }

  public void putFinalStatus(final CommandId commandId,
                             final Optional<QueuedCommandStatus> queuedCommandStatus,
                             final CommandStatus status) {
    statusStore.put(commandId, status);
    queuedCommandStatus.ifPresent(s -> s.setFinalStatus(status));
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
      final Optional<QueuedCommandStatus> queuedCommandStatus,
      final Mode mode,
      final List<QueryId> restoredQueries
  ) {
    try {
      final String statementString = command.getStatement();
      maybeHandleLegacyDropCommand(commandId, command);
      putStatus(
          commandId,
          queuedCommandStatus,
          new CommandStatus(CommandStatus.Status.PARSING, "Parsing statement"));
      final Statement statement = statementParser.parseSingleStatement(statementString);
      putStatus(
          commandId,
          queuedCommandStatus,
          new CommandStatus(CommandStatus.Status.EXECUTING, "Executing statement")
      );
      executeStatement(
          statement, command, commandId, queuedCommandStatus, mode, restoredQueries);
    } catch (final WakeupException exception) {
      throw exception;
    } catch (final Exception exception) {
      log.error("Failed to handle: " + command, exception);
      final CommandStatus errorStatus = new CommandStatus(
          CommandStatus.Status.ERROR,
          ExceptionUtil.stackTraceToString(exception)
      );
      putFinalStatus(commandId, queuedCommandStatus, errorStatus);
    }
  }

  private void executeStatement(
      final Statement statement,
      final Command command,
      final CommandId commandId,
      final Optional<QueuedCommandStatus> queuedCommandStatus,
      final Mode mode,
      final List<QueryId> restoredQueries
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
          statementStr,
          mode,
          restoredQueries);
      if (successMessage == null) {
        return;
      }
    } else if (statement instanceof InsertInto) {
      successMessage = handleInsertInto(
          (InsertInto) statement,
          command,
          statementStr,
          mode,
          restoredQueries);
      if (successMessage == null) {
        return;
      }
    } else if (statement instanceof TerminateQuery) {
      terminateQuery((TerminateQuery) statement, mode);
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
    putFinalStatus(commandId, queuedCommandStatus, successStatus);
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
          persistentQueryMd.start();
        }
      }
    } else {
      throw new KsqlException("No statements received for LOAD FROM FILE.");
    }

  }

  private String handleCreateAsSelect(
      final CreateAsSelect statement,
      final Command command,
      final String statementStr,
      final Mode mode,
      final List<QueryId> restoredQueries
  ) throws Exception {
    final QuerySpecification querySpecification =
        (QuerySpecification) statement.getQuery().getQueryBody();
    final Query query = ksqlEngine.addInto(
        querySpecification,
        statement.getName().getSuffix(),
        statement.getQuery().getLimit(),
        statement.getProperties(),
        statement.getPartitionByColumn(),
        true
    );
    if (startQuery(
        statementStr,
        query,
        command,
        mode,
        restoredQueries)) {
      return statement instanceof CreateTableAsSelect
             ? "Table created and running"
             : "Stream created and running";
    }

    return null;
  }

  private String handleInsertInto(
      final InsertInto statement,
      final Command command,
      final String statementStr,
      final Mode mode,
      final List<QueryId> restoredQueries) throws Exception {
    final QuerySpecification querySpecification =
        (QuerySpecification) statement.getQuery().getQueryBody();
    final Query query = ksqlEngine.addInto(
        querySpecification,
        statement.getTarget().getSuffix(),
        statement.getQuery().getLimit(),
        new HashMap<>(),
        Optional.empty(),
        false
    );
    if (startQuery(
        statementStr,
        query,
        command,
        mode,
        restoredQueries)) {
      return "Insert Into query is running.";
    }

    return null;
  }

  private boolean startQuery(
      final String queryString,
      final Query query,
      final Command command,
      final Mode mode,
      final List<QueryId> restoredQueries
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
      if (mode == Mode.EXECUTE) {
        persistentQueryMd.start();
      } else {
        restoredQueries.add(persistentQueryMd.getQueryId());
      }
      return true;
    } else {
      throw new Exception(String.format(
          "Unexpected query metadata type: %s; was expecting %s",
          queryMetadata.getClass().getCanonicalName(),
          PersistentQueryMetadata.class.getCanonicalName()
      ));
    }
  }

  private void terminateQuery(
      final TerminateQuery terminateQuery,
      final Mode mode) throws Exception {
    final QueryId queryId = terminateQuery.getQueryId();
    if (!ksqlEngine.terminateQuery(queryId, mode == Mode.EXECUTE)) {
      throw new Exception(String.format("No running query with id %s was found", queryId));
    }
  }

  private boolean is4Dot1Command(final Command command) {
    return !command.hasOriginalProperties();
  }

  private void maybeHandleLegacyDropCommand(final CommandId commandId, final Command command) {
    if (!is4Dot1Command(command)) {
      return;
    }
    if (!commandId.getAction().equals(Action.DROP)) {
      return;
    }
    if (commandId.getType().equals(Type.STREAM) || commandId.getType().equals(Type.TABLE)) {
      terminateQueriesForSource(commandId.getEntity());
    }
  }

  private void terminateQueriesForSource(final String sourceName) {
    final MetaStore metaStore = ksqlEngine.getMetaStore();
    if (metaStore.getSource(sourceName) == null) {
      return;
    }
    final Collection<String> queriesWithSink = metaStore.getQueriesWithSink(sourceName);
    if (queriesWithSink.size() == 0) {
      return;
    }
    final QueryId queryId = new QueryId(queriesWithSink.iterator().next());
    ksqlEngine.terminateQuery(queryId, false);
  }
}
