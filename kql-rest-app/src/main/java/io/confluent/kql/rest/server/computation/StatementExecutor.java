/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest.server.computation;

import io.confluent.kql.KQLEngine;
import io.confluent.kql.ddl.DDLConfig;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.tree.CreateStream;
import io.confluent.kql.parser.tree.CreateStreamAsSelect;
import io.confluent.kql.parser.tree.CreateTable;
import io.confluent.kql.parser.tree.CreateTableAsSelect;
import io.confluent.kql.parser.tree.CreateTopic;
import io.confluent.kql.parser.tree.Query;
import io.confluent.kql.parser.tree.QuerySpecification;
import io.confluent.kql.parser.tree.Relation;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.parser.tree.Table;
import io.confluent.kql.parser.tree.TerminateQuery;
import io.confluent.kql.physical.PhysicalPlanBuilder;
import io.confluent.kql.planner.plan.AggregateNode;
import io.confluent.kql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.kql.planner.plan.OutputNode;
import io.confluent.kql.planner.plan.PlanNode;
import io.confluent.kql.rest.server.StatementParser;
import io.confluent.kql.rest.server.TopicUtil;
import io.confluent.kql.structured.SchemaKStream;
import io.confluent.kql.structured.SchemaKTable;
import io.confluent.kql.util.KQLException;
import io.confluent.kql.util.Pair;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles the actual execution (or delegation to KSQL core) of all distributed statements, as well as tracking
 * their statuses as things move along.
 */
public class StatementExecutor {

  private static final Logger log = LoggerFactory.getLogger(StatementExecutor.class);

  private static final Pattern UNQUOTED_TERMINATE_PATTERN =
      Pattern.compile("\\s*TERMINATE\\s+([A-Z][A-Z0-9_@:]*)\\s*;?\\s*", Pattern.CASE_INSENSITIVE);
  private static final Pattern DOUBLE_QUOTED_TERMINATE_PATTERN =
      Pattern.compile("\\s*TERMINATE\\s+\"((\"\"|[^\"])*)\"\\s*;?\\s*", Pattern.CASE_INSENSITIVE);
  private static final Pattern BACK_QUOTED_TERMINATE_PATTERN =
      Pattern.compile("\\s*TERMINATE\\s+`((``|[^`])*)`\\s*;?\\s*", Pattern.CASE_INSENSITIVE);

  private final TopicUtil topicUtil;
  private final KQLEngine kqlEngine;
  private final StatementParser statementParser;
  private final Map<String, StatementStatus> statusStore;

  public StatementExecutor(TopicUtil topicUtil, KQLEngine kqlEngine, StatementParser statementParser) {
    this.topicUtil = topicUtil;
    this.kqlEngine = kqlEngine;
    this.statementParser = statementParser;

    this.statusStore = new HashMap<>();
  }

  /**
   * Execute a series of statements. The only difference between this and multiple consecutive calls to
   * {@link #handleStatement(String, String)} is that terminated queries will not be instantiated at all (as long as
   * the statements responsible for both their creation and termination are both present in {@code priorCommands}.
   * @param priorCommands A map of statements to execute, where keys are statement IDs and values are the actual strings
   *                      of the statements.
   * @throws Exception TODO: Refine this.
   */
  public void handleStatements(LinkedHashMap<String, String> priorCommands) throws Exception {
    Map<String, String> terminatedQueries = getTerminatedQueries(priorCommands);
    for (Map.Entry<String, String> commandEntry : priorCommands.entrySet()) {
      Statement statement = statementParser.parseSingleStatement(commandEntry.getValue());
      boolean isCreateStream = statement instanceof CreateStreamAsSelect;
      boolean isCreateTable = statement instanceof CreateTableAsSelect;
      // In this case, we want to fully deduce the metadata for the created stream/table and then store that in the
      // metastore, all without actually running it.
      if ((isCreateStream || isCreateTable) && terminatedQueries.containsKey(commandEntry.getKey() + "_QUERY")) {
        Query query;
        if (isCreateStream) {
          CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
          QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
          query = kqlEngine.addInto(
              createStreamAsSelect.getQuery(),
              querySpecification,
              createStreamAsSelect.getName().getSuffix(),
              createStreamAsSelect.getProperties()
          );
        } else {
          CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
          QuerySpecification querySpecification = (QuerySpecification) createTableAsSelect.getQuery().getQueryBody();
          query = kqlEngine.addInto(
              createTableAsSelect.getQuery(),
              querySpecification,
              createTableAsSelect.getName().getSuffix(),
              createTableAsSelect.getProperties()
          );
        }
        String queryId = (commandEntry.getKey() + "_query").toUpperCase();
        List<Pair<String, Query>> queryList = Collections.singletonList(new Pair<>(queryId, query));
        PlanNode logicalPlan =
            kqlEngine.getQueryEngine().buildLogicalPlans(kqlEngine.getMetaStore(), queryList).get(0).getRight();
        KStreamBuilder builder = new KStreamBuilder();
        PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(builder);
        SchemaKStream schemaKStream = physicalPlanBuilder.buildPhysicalPlan(logicalPlan);
        OutputNode outputNode = physicalPlanBuilder.getPlanSink();
        if (outputNode instanceof KQLStructuredDataOutputNode) {
          KQLStructuredDataOutputNode outputKafkaTopicNode = (KQLStructuredDataOutputNode) outputNode;
          if (kqlEngine.getMetaStore().getTopic(outputKafkaTopicNode.getKafkaTopicName()) == null) {
            kqlEngine.getMetaStore().putTopic(outputKafkaTopicNode.getKqlTopic());
          }
          StructuredDataSource sinkDataSource;
          if (schemaKStream instanceof SchemaKTable) {
            sinkDataSource =
                new KQLTable(outputKafkaTopicNode.getId().toString(),
                    outputKafkaTopicNode.getSchema(),
                    outputKafkaTopicNode.getKeyField(),
                    outputKafkaTopicNode.getKqlTopic(),
                    outputKafkaTopicNode.getId().toString() + "_statestore",
                    outputNode.getSource() instanceof AggregateNode
                );
          } else {
            sinkDataSource =
                new KQLStream(outputKafkaTopicNode.getId().toString(),
                    outputKafkaTopicNode.getSchema(),
                    outputKafkaTopicNode.getKeyField(),
                    outputKafkaTopicNode.getKqlTopic()
                );
          }

          kqlEngine.getMetaStore().putSource(sinkDataSource);
        } else {
          throw new Exception(String.format(
              "Unexpected output node type: '%s'",
              outputNode.getClass().getCanonicalName()
          ));
        }
        statusStore.put(
            commandEntry.getKey(),
            new StatementStatus(StatementStatus.Status.TERMINATED, "Query terminated")
        );
        statusStore.put(
            terminatedQueries.get(commandEntry.getKey() + "_QUERY"),
            new StatementStatus(StatementStatus.Status.SUCCESS, "Termination request granted")
        );
      } else if (!(statement instanceof TerminateQuery)) {
        log.info("Executing prior statement: '{}'", commandEntry.getValue());
        try {
          handleStatement(commandEntry.getValue(), commandEntry.getKey());
        } catch (Exception exception) {
          log.warn("Failed to execute statement due to exception", exception);
        }
      }
    }

    for (String terminateCommand : terminatedQueries.values()) {
      if (!statusStore.containsKey(terminateCommand)) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        new Exception("Query not found").printStackTrace(printWriter);
        statusStore.put(terminateCommand, new StatementStatus(StatementStatus.Status.ERROR, stringWriter.toString()));
      }
    }
  }

  /**
   * Attempt to execute a single statement.
   * @param statementString The string containing the statement to be executed.
   * @param statementId The ID to be used to track the status of the statement.
   * @throws Exception TODO: Refine this.
   */
  public void handleStatement(String statementString, String statementId) throws Exception {
    try {
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.PARSING, "Parsing statement"));
      Statement statement = statementParser.parseSingleStatement(statementString);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.EXECUTING, "Executing statement"));
      handleStatement(statement, statementString, statementId);
    } catch (WakeupException exception) {
      throw exception;
    } catch (Exception exception) {
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      exception.printStackTrace(printWriter);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.ERROR, stringWriter.toString()));
    }
  }

  /**
   * Get details on the statuses of all the statements handled thus far.
   * @return A map detailing the current statuses of all statements that the handler has executed (or attempted to
   * execute).
   */
  public Map<String, StatementStatus> getStatuses() {
    return new HashMap<>(statusStore);
  }

  /**
   * @param statementId The ID of the statement to check the status of.
   * @return Information on the status of the statement with the given ID, if one exists.
   */
  public Optional<StatementStatus> getStatus(String statementId) {
    return Optional.ofNullable(statusStore.get(statementId));
  }

  /**
   * Register the existence of a new statement that has been written to the command topic. All other statement status
   * information is updated exclusively by the current {@link StatementExecutor} instance, but in the
   * (unlikely but possible) event that a statement is written to the command topic but never picked up by this
   * instance, it should be possible to know that it was at least written to the topic in the first place.
   * @param statementId The ID of the statement that has been written to the command topic.
   */
  public void registerQueuedStatement(String statementId) {
    statusStore.put(
        statementId,
        new StatementStatus(StatementStatus.Status.QUEUED, "Statement written to command topic")
    );
  }

  private Map<String, String> getTerminatedQueries(Map<String, String> commands) {
    Map<String, String> result = new HashMap<>();

    for (Map.Entry<String, String> commandEntry : commands.entrySet()) {
      String commandId = commandEntry.getKey();
      String command = commandEntry.getValue();
      Matcher unquotedMatcher = UNQUOTED_TERMINATE_PATTERN.matcher(command);
      if (unquotedMatcher.matches()) {
        result.put(unquotedMatcher.group(1).toUpperCase(), commandId);
      } else {
        Matcher doubleQuotedMatcher = DOUBLE_QUOTED_TERMINATE_PATTERN.matcher(command);
        if (doubleQuotedMatcher.matches()) {
          result.put(doubleQuotedMatcher.group(1).replace("\"\"", "\""), commandId);
        } else {
          Matcher backQuotedMatcher = BACK_QUOTED_TERMINATE_PATTERN.matcher(command);
          if (backQuotedMatcher.matches()) {
            result.put(backQuotedMatcher.group(1).replace("``", "`"), commandId);
          }
        }
      }
    }

    return result;
  }

  // Copied from io.confluent.kql.ddl.DDLEngine
  private String enforceString(final String propertyName, final String propertyValue) {
    if (!propertyValue.startsWith("'") && !propertyValue.endsWith("'")) {
      throw new KQLException(propertyName + " value is string and should be enclosed between " + "\"'\".");
    }
    return propertyValue.substring(1, propertyValue.length() - 1);
  }

  private void handleStatement(Statement statement, String statementStr, String statementId) throws Exception {
    if (statement instanceof CreateTopic) {
      CreateTopic createTopic = (CreateTopic) statement;
      String kafkaTopicName =
          createTopic.getProperties().get(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY).toString();
      kafkaTopicName = enforceString(DDLConfig.KAFKA_TOPIC_NAME_PROPERTY, kafkaTopicName);
      topicUtil.ensureTopicExists(kafkaTopicName);
      kqlEngine.getDdlEngine().createTopic(createTopic);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.SUCCESS, "Topic created"));
    } else if (statement instanceof CreateStream) {
      CreateStream createStream = (CreateStream) statement;
      String streamName = createStream.getName().getSuffix();
      topicUtil.ensureTopicExists(streamName);
      kqlEngine.getDdlEngine().createStream(createStream);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.SUCCESS, "Stream created"));
    } else if (statement instanceof CreateTable) {
      CreateTable createTable = (CreateTable) statement;
      String tableName = createTable.getName().getSuffix();
      topicUtil.ensureTopicExists(tableName);
      kqlEngine.getDdlEngine().createTable(createTable);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.SUCCESS, "Table created"));
    } else if (statement instanceof CreateStreamAsSelect) {
      CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
      QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
      Query query = kqlEngine.addInto(
          createStreamAsSelect.getQuery(),
          querySpecification,
          createStreamAsSelect.getName().getSuffix(),
          createStreamAsSelect.getProperties()
      );
      String streamName = createStreamAsSelect.getName().getSuffix();
      topicUtil.ensureTopicExists(streamName);
      String queryId = statementId + "_QUERY";
      startQuery(statementStr, query, queryId);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.RUNNING, "Stream created and running"));
    } else if (statement instanceof CreateTableAsSelect) {
      CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
      QuerySpecification querySpecification = (QuerySpecification) createTableAsSelect.getQuery().getQueryBody();
      Query query = kqlEngine.addInto(
          createTableAsSelect.getQuery(),
          querySpecification,
          createTableAsSelect.getName().getSuffix(),
          createTableAsSelect.getProperties()
      );
      String tableName = createTableAsSelect.getName().getSuffix();
      topicUtil.ensureTopicExists(tableName);
      String queryId = statementId + "_QUERY";
      startQuery(statementStr, query, queryId);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.RUNNING, "Table created and running"));
    } else if (statement instanceof TerminateQuery) {
      terminateQuery((TerminateQuery) statement);
      statusStore.put(statementId, new StatementStatus(StatementStatus.Status.SUCCESS, "Termination request granted"));
    } else {
      throw new Exception(String.format(
          "Unexpected statement type: %s",
          statement.getClass().getName()
      ));
    }
  }

  private void startQuery(String queryString, Query query, String queryId) throws Exception {
    if (query.getQueryBody() instanceof QuerySpecification) {
      QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
      Optional<Relation> into = querySpecification.getInto();
      if (into.isPresent() && into.get() instanceof Table) {
        Table table = (Table) into.get();
        if (kqlEngine.getMetaStore().getSource(table.getName().getSuffix()) != null) {
          throw new Exception(String.format(
              "Sink specified in INTO clause already exists: %s",
              table.getName().getSuffix().toUpperCase()
          ));
        }
      }
    }

    kqlEngine.runMultipleQueries(false, queryString);
  }

  private void terminateQuery(TerminateQuery terminateQuery) throws Exception {
    String queryId = terminateQuery.getQueryId().toString();
    if (!kqlEngine.terminateQuery(queryId)) {
      throw new Exception(String.format("No running query with id '%s' was found", queryId));
    }
    String queryStatementId = queryId.substring(0, queryId.length() - "_QUERY".length());
    statusStore.put(queryStatementId, new StatementStatus(StatementStatus.Status.TERMINATED, "Query terminated"));
  }
}
