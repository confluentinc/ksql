/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.ddl.DDLConfig;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.CreateTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.planner.plan.KSQLStructuredDataOutputNode;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.TopicUtil;
import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles the actual execution (or delegation to KSQL core) of all distributed statements, as well as tracking
 * their statuses as things move along.
 */
public class StatementExecutor {

  private static final Logger log = LoggerFactory.getLogger(StatementExecutor.class);

  private static final Pattern TERMINATE_PATTERN =
      Pattern.compile("\\s*TERMINATE\\s+([0-9]+)\\s*;?\\s*");

  private final TopicUtil topicUtil;
  private final KSQLEngine ksqlEngine;
  private final StatementParser statementParser;
  private final Map<CommandId, CommandStatus> statusStore;
  private final Map<CommandId, CommandStatusFuture> statusFutures;

  public StatementExecutor(TopicUtil topicUtil, KSQLEngine ksqlEngine, StatementParser statementParser) {
    this.topicUtil = topicUtil;
    this.ksqlEngine = ksqlEngine;
    this.statementParser = statementParser;

    this.statusStore = new HashMap<>();
    this.statusFutures = new HashMap<>();
  }

  /**
   * Execute a series of statements. The only difference between this and multiple consecutive calls to
   * {@link #handleStatement(String, CommandId)} is that terminated queries will not be instantiated at all
   * (as long as the statements responsible for both their creation and termination are both present in
   * {@code priorCommands}.
   * @param priorCommands A map of statements to execute, where keys are statement IDs and values are the actual strings
   *                      of the statements.
   * @throws Exception TODO: Refine this.
   */
  public void handleStatements(LinkedHashMap<CommandId, String> priorCommands) throws Exception {
    Map<Long, CommandId> terminatedQueries = getTerminatedQueries(priorCommands);

    for (Map.Entry<CommandId, String> commandEntry : priorCommands.entrySet()) {
      String statementString = commandEntry.getValue();
      Statement statement = statementParser.parseSingleStatement(statementString);
      if (!(statement instanceof TerminateQuery)) {
        log.info("Executing prior statement: '{}'", commandEntry.getValue());
        try {
          handleStatement(commandEntry.getValue(), commandEntry.getKey(), terminatedQueries);
        } catch (Exception exception) {
          log.warn("Failed to execute statement due to exception", exception);
        }
      }
    }

    for (CommandId terminateCommand : terminatedQueries.values()) {
      if (!statusStore.containsKey(terminateCommand)) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        new Exception("Query not found").printStackTrace(printWriter);
        statusStore.put(terminateCommand, new CommandStatus(CommandStatus.Status.ERROR, stringWriter.toString()));
      }
    }
  }

  /**
   * Attempt to execute a single statement.
   * @param statementString The string containing the statement to be executed
   * @param commandId The ID to be used to track the status of the command
   * @throws Exception TODO: Refine this.
   */
  public void handleStatement(
      String statementString,
      CommandId commandId
  ) throws Exception {
    handleStatement(statementString, commandId, null);
  }

  /**
   * Get details on the statuses of all the statements handled thus far.
   * @return A map detailing the current statuses of all statements that the handler has executed (or attempted to
   * execute).
   */
  public Map<CommandId, CommandStatus> getStatuses() {
    return new HashMap<>(statusStore);
  }

  /**
   * @param statementId The ID of the statement to check the status of.
   * @return Information on the status of the statement with the given ID, if one exists.
   */
  public Optional<CommandStatus> getStatus(CommandId statementId) {
    return Optional.ofNullable(statusStore.get(statementId));
  }

  /**
   * Register the existence of a new statement that has been written to the command topic. All other statement status
   * information is updated exclusively by the current {@link StatementExecutor} instance, but in the
   * (unlikely but possible) event that a statement is written to the command topic but never picked up by this
   * instance, it should be possible to know that it was at least written to the topic in the first place.
   * @param commandId The ID of the statement that has been written to the command topic.
   */
  public Future<CommandStatus> registerQueuedStatement(CommandId commandId) {
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
        result = new CommandStatusFuture(commandId);
        statusFutures.put(commandId, result);
        return result;
      }
    }
  }

  private void completeStatusFuture(CommandId commandId, CommandStatus commandStatus) {
    synchronized (statusFutures) {
      CommandStatusFuture statusFuture = statusFutures.get(commandId);
      if (statusFuture != null) {
        statusFuture.complete(commandStatus);
      } else {
        CommandStatusFuture newStatusFuture = new CommandStatusFuture(commandId);
        newStatusFuture.complete(commandStatus);
        statusFutures.put(commandId, newStatusFuture);
      }
    }
  }

  private Map<Long, CommandId> getTerminatedQueries(Map<CommandId, String> commands) {
    Map<Long, CommandId> result = new HashMap<>();

    for (Map.Entry<CommandId, String> commandEntry : commands.entrySet()) {
      CommandId commandId = commandEntry.getKey();
      String command = commandEntry.getValue();
      Matcher terminateMatcher = TERMINATE_PATTERN.matcher(command);
      if (terminateMatcher.matches()) {
        Long queryId = Long.parseLong(terminateMatcher.group(1));
        result.put(queryId, commandId);
      }
    }

    return result;
  }

  // Copied from io.confluent.ksql.ddl.DDLEngine
  private String enforceString(final String propertyName, final String propertyValue) {
    if (!propertyValue.startsWith("'") && !propertyValue.endsWith("'")) {
      throw new KSQLException(propertyName + " value is string and should be enclosed between " + "\"'\".");
    }
    return propertyValue.substring(1, propertyValue.length() - 1);
  }

  /**
   * Attempt to execute a single statement.
   * @param statementString The string containing the statement to be executed
   * @param commandId The ID to be used to track the status of the command
   * @param terminatedQueries An optional map from terminated query IDs to the commands that requested their termination
   * @throws Exception TODO: Refine this.
   */
  private void handleStatement(
      String statementString,
      CommandId commandId,
      Map<Long, CommandId> terminatedQueries
  ) throws Exception {
    try {
      statusStore.put(commandId, new CommandStatus(CommandStatus.Status.PARSING, "Parsing statement"));
      Statement statement = statementParser.parseSingleStatement(statementString);
      statusStore.put(commandId, new CommandStatus(CommandStatus.Status.EXECUTING, "Executing statement"));
      handleStatement(statement, statementString, commandId, terminatedQueries);
    } catch (WakeupException exception) {
      throw exception;
    } catch (Exception exception) {
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      exception.printStackTrace(printWriter);
      CommandStatus errorStatus = new CommandStatus(CommandStatus.Status.ERROR, stringWriter.toString());
      statusStore.put(commandId, errorStatus);
      completeStatusFuture(commandId, errorStatus);
    }
  }

  private void handleStatement(
      Statement statement,
      String statementStr,
      CommandId commandId,
      Map<Long, CommandId> terminatedQueries
  ) throws Exception {
    String successMessage;
    if (statement instanceof CreateTopic) {
      CreateTopic createTopic = (CreateTopic) statement;
      String kafkaTopicName =
          createTopic.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY).toString();
      kafkaTopicName = enforceString(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY, kafkaTopicName);
      ksqlEngine.getDdlEngine().createTopic(createTopic);
      successMessage = "Topic created";
    } else if (statement instanceof CreateStream) {
      CreateStream createStream = (CreateStream) statement;
      String streamName = createStream.getName().getSuffix();
      ksqlEngine.getDdlEngine().createStream(createStream);
      successMessage = "Stream created";
    } else if (statement instanceof CreateTable) {
      CreateTable createTable = (CreateTable) statement;
      String tableName = createTable.getName().getSuffix();
      ksqlEngine.getDdlEngine().createTable(createTable);
      successMessage = "Table created";
    } else if (statement instanceof CreateStreamAsSelect) {
      CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
      QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
      Query query = ksqlEngine.addInto(
          createStreamAsSelect.getQuery(),
          querySpecification,
          createStreamAsSelect.getName().getSuffix(),
          createStreamAsSelect.getProperties()
      );
      String streamName = createStreamAsSelect.getName().getSuffix();
      topicUtil.ensureTopicExists(streamName);
      if (startQuery(statementStr, query, commandId, terminatedQueries)) {
        successMessage = "Stream created and running";
      } else {
        return;
      }
    } else if (statement instanceof CreateTableAsSelect) {
      CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
      QuerySpecification querySpecification = (QuerySpecification) createTableAsSelect.getQuery().getQueryBody();
      Query query = ksqlEngine.addInto(
          createTableAsSelect.getQuery(),
          querySpecification,
          createTableAsSelect.getName().getSuffix(),
          createTableAsSelect.getProperties()
      );
      String tableName = createTableAsSelect.getName().getSuffix();
      if (startQuery(statementStr, query, commandId, terminatedQueries)) {
        successMessage = "Table created and running";
      } else {
        return;
      }
    } else if (statement instanceof TerminateQuery) {
      terminateQuery((TerminateQuery) statement);
      successMessage = "Termination request granted";
    } else {
      throw new Exception(String.format(
          "Unexpected statement type: %s",
          statement.getClass().getName()
      ));
    }
    CommandStatus successStatus = new CommandStatus(CommandStatus.Status.SUCCESS, successMessage);
    statusStore.put(commandId, successStatus);
    completeStatusFuture(commandId, successStatus);
  }

  private boolean startQuery(
      String queryString,
      Query query,
      CommandId commandId,
      Map<Long, CommandId> terminatedQueries
  ) throws Exception {
    if (query.getQueryBody() instanceof QuerySpecification) {
      QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
      Optional<Relation> into = querySpecification.getInto();
      if (into.isPresent() && into.get() instanceof Table) {
        Table table = (Table) into.get();
        if (ksqlEngine.getMetaStore().getSource(table.getName().getSuffix()) != null) {
          throw new Exception(String.format(
              "Sink specified in INTO clause already exists: %s",
              table.getName().getSuffix().toUpperCase()
          ));
        }
      }
    }

    QueryMetadata queryMetadata = ksqlEngine.buildMultipleQueries(false, queryString).get(0);

    if (queryMetadata instanceof PersistentQueryMetadata) {
      PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
      long queryId = persistentQueryMetadata.getId();

      if (terminatedQueries != null && terminatedQueries.containsKey(queryId)) {
        CommandId terminateId = terminatedQueries.get(queryId);
        statusStore.put(terminateId, new CommandStatus(CommandStatus.Status.SUCCESS, "Termination request granted"));
        statusStore.put(commandId, new CommandStatus(CommandStatus.Status.TERMINATED, "Query terminated"));
        ksqlEngine.terminateQuery(queryId, false);
        return false;
      } else {
        persistentQueryMetadata.getKafkaStreams().start();
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

  private void terminateQuery(TerminateQuery terminateQuery) throws Exception {
    long queryId = terminateQuery.getQueryId();
    QueryMetadata queryMetadata = ksqlEngine.getPersistentQueries().get(queryId);
    if (!ksqlEngine.terminateQuery(queryId, true)) {
      throw new Exception(String.format("No running query with id %d was found", queryId));
    }

    CommandId.Type commandType;
    DataSource.DataSourceType sourceType = queryMetadata.getOutputNode().getTheSourceNode().getDataSourceType();
    switch (sourceType) {
      case KTABLE:
        commandType = CommandId.Type.TABLE;
        break;
      case KSTREAM:
        commandType = CommandId.Type.STREAM;
        break;
      default:
        throw new Exception(String.format("Unexpected source type for running query: %s", sourceType));
    }

    String queryEntity = ((KSQLStructuredDataOutputNode) queryMetadata.getOutputNode()).getKsqlTopic().getName();

    CommandId queryStatementId = new CommandId(commandType, queryEntity);
    statusStore.put(queryStatementId, new CommandStatus(CommandStatus.Status.TERMINATED, "Query terminated"));
  }

  private class CommandStatusFuture implements Future<CommandStatus> {

    private final CommandId commandId;
    private final AtomicReference<CommandStatus> result;

    public CommandStatusFuture(CommandId commandId) {
      this.commandId = commandId;
      this.result = new AtomicReference<>(null);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false; // TODO: Is an implementation of this method necessary?
    }

    @Override
    public boolean isCancelled() {
      return false; // TODO: Is an implementation of this method necessary?
    }

    @Override
    public boolean isDone() {
      return result.get() != null;
    }

    @Override
    public CommandStatus get() throws InterruptedException {
      synchronized (result) {
        while (result.get() == null) {
          result.wait();
        }
        removeFromFutures();
        return result.get();
      }
    }

    @Override
    public CommandStatus get(long timeout, TimeUnit unit)
        throws InterruptedException, TimeoutException {
      long endTimeMs = System.currentTimeMillis() + unit.toMillis(timeout);
      synchronized (result) {
        while (result.get() == null) {
          result.wait(Math.max(1, endTimeMs - System.currentTimeMillis()));
        }
        if (result.get() == null) {
          throw new TimeoutException();
        }
        removeFromFutures();
        return result.get();
      }
    }

    private void complete(CommandStatus result) {
      synchronized (this.result) {
        this.result.set(result);
        this.result.notifyAll();
      }
    }

    private void removeFromFutures() {
      synchronized (statusFutures) {
        statusFutures.remove(commandId);
      }
    }
  }
}
