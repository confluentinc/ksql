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

package io.confluent.ksql.rest.server.resources;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.CreateTableCommand;
import io.confluent.ksql.ddl.commands.DDLCommand;
import io.confluent.ksql.ddl.commands.DDLCommandExec;
import io.confluent.ksql.ddl.commands.DDLCommandResult;
import io.confluent.ksql.ddl.commands.DropSourceCommand;
import io.confluent.ksql.ddl.commands.DropTopicCommand;
import io.confluent.ksql.ddl.commands.RegisterTopicCommand;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListRegisteredTopics;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.ErrorMessageEntity;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.AvroUtil;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
import io.confluent.ksql.util.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

@Path("/ksql")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class KsqlResource {

  private static final org.slf4j.Logger log = LoggerFactory.getLogger(KsqlResource.class);

  private final KsqlEngine ksqlEngine;
  private final CommandStore commandStore;
  private final StatementExecutor statementExecutor;
  private final long distributedCommandResponseTimeout;

  public KsqlResource(
      KsqlEngine ksqlEngine,
      CommandStore commandStore,
      StatementExecutor statementExecutor,
      long distributedCommandResponseTimeout
  ) {
    this.ksqlEngine = ksqlEngine;
    this.commandStore = commandStore;
    this.statementExecutor = statementExecutor;
    this.distributedCommandResponseTimeout = distributedCommandResponseTimeout;
    registerDdlCommandTasks();
  }

  @POST
  public Response handleKsqlStatements(KsqlRequest request) throws Exception {
    KsqlEntityList result = new KsqlEntityList();
    try {
      List<Statement> parsedStatements = ksqlEngine.getStatements(request.getKsql());
      List<String> statementStrings = getStatementStrings(request.getKsql());
      Map<String, Object> streamsProperties = request.getStreamsProperties();
      if (parsedStatements.size() != statementStrings.size()) {
        throw new Exception(String.format(
            "Size of parsed statements and statement strings differ; %d vs. %d, respectively",
            parsedStatements.size(),
            statementStrings.size()
        ));
      }

      for (int i = 0; i < parsedStatements.size(); i++) {
        String statementText = statementStrings.get(i);
        result.add(executeStatement(statementText, parsedStatements.get(i), streamsProperties));
      }
    } catch (Exception exception) {
      log.error("Failed to handle POST:" + request, exception);
      result.add(new ErrorMessageEntity(request.getKsql(), exception));
    }

    return Response.ok(result).build();
  }

  public List<String> getStatementStrings(String ksqlString) {
    List<SqlBaseParser.SingleStatementContext> statementContexts =
        new KsqlParser().getStatements(ksqlString);
    List<String> result = new ArrayList<>(statementContexts.size());
    for (SqlBaseParser.SingleStatementContext statementContext : statementContexts) {
      // Taken from http://stackoverflow
      // .com/questions/16343288/how-do-i-get-the-original-text-that-an-antlr4-rule-matched
      CharStream charStream = statementContext.start.getInputStream();
      result.add(
          charStream.getText(
              new Interval(
                  statementContext.start.getStartIndex(),
                  statementContext.stop.getStopIndex()
              )
          )
      );
    }
    return result;
  }

  public KsqlEngine getKsqlEngine() {
    return ksqlEngine;
  }

  private KsqlEntity executeStatement(
      String statementText,
      Statement statement,
      Map<String, Object> streamsProperties
  ) throws KsqlException {
    if (statement instanceof ListTopics) {
      return listTopics(statementText);
    } else if (statement instanceof ListRegisteredTopics) {
      return listRegisteredTopics(statementText);
    } else if (statement instanceof ListStreams) {
      return listStreams(statementText);
    } else if (statement instanceof ListTables) {
      return listTables(statementText);
    } else if (statement instanceof ListQueries) {
      return showQueries(statementText);
    } else if (statement instanceof ShowColumns) {
      ShowColumns showColumns = (ShowColumns) statement;
      if (showColumns.isTopic()) {
        return describeTopic(statementText, showColumns.getTable().getSuffix());
      }
      return describe(showColumns.getTable().getSuffix(), showColumns.isExtended());
    } else if (statement instanceof ListProperties) {
      return listProperties(statementText);
    } else if (statement instanceof Explain) {
      Explain explain = (Explain) statement;
      return getStatementExecutionPlan(explain, statementText);
    } else if (statement instanceof RunScript) {
      return distributeStatement(statementText, statement, streamsProperties);
    } else if (statement instanceof RegisterTopic
               || statement instanceof CreateStream
               || statement instanceof CreateTable
               || statement instanceof CreateStreamAsSelect
               || statement instanceof CreateTableAsSelect
               || statement instanceof TerminateQuery
               || statement instanceof DropTopic
               || statement instanceof DropStream
               || statement instanceof DropTable
    ) {
      if (statement instanceof AbstractStreamCreateStatement) {
        AbstractStreamCreateStatement streamCreateStatement = (AbstractStreamCreateStatement)
            statement;
        Pair<AbstractStreamCreateStatement, String> avroCheckResult =
            maybeAddFieldsFromSchemaRegistry(streamCreateStatement, streamsProperties);

        if (avroCheckResult.getRight() != null) {
          statement = avroCheckResult.getLeft();
          statementText = avroCheckResult.getRight();
        }
      }
      //Sanity check for the statement before distributing it.
      validateStatement(statement, statementText, streamsProperties);
      return distributeStatement(statementText, statement, streamsProperties);
    } else {
      if (statement != null) {
        throw new KsqlException(String.format(
            "Cannot handle statement of type '%s'",
            statement.getClass().getSimpleName()
        ));
      } else {
        throw new KsqlException(String.format(
            "Unable to execute statement '%s'",
            statementText
        ));
      }
    }
  }

  /**
   * Validate the statement by creating the execution plan for it.
   */
  private void validateStatement(
      Statement statement, String statementText,
      Map<String, Object> streamsProperties
  ) throws KsqlException {
    getStatementExecutionPlan(null, statement, statementText, streamsProperties);
  }

  private CommandStatusEntity distributeStatement(
      String statementText,
      Statement statement,
      Map<String, Object> streamsProperties
  ) throws KsqlException {
    CommandId commandId =
        commandStore.distributeStatement(statementText, statement, streamsProperties);
    CommandStatus commandStatus;
    try {
      commandStatus = statementExecutor.registerQueuedStatement(commandId)
          .get(distributedCommandResponseTimeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException exception) {
      log.warn(
          "Timeout to get commandStatus, waited {} milliseconds:, statementText:" + statementText,
          distributedCommandResponseTimeout, exception
      );
      commandStatus = statementExecutor.getStatus(commandId).get();
    } catch (Exception e) {
      throw new KsqlException(
          String.format(
              "Could not write the statement '%s' into the command " + "topic.", statementText
          ),
          e
      );
    }
    return new CommandStatusEntity(statementText, commandId, commandStatus);
  }

  private KafkaTopicsList listTopics(String statementText) {
    KafkaTopicClient client = ksqlEngine.getTopicClient();
    try (KafkaConsumerGroupClient kafkaConsumerGroupClient = new KafkaConsumerGroupClientImpl(
        ksqlEngine.getKsqlConfig())) {
      return KafkaTopicsList.build(
          statementText,
          getKsqlTopics(),
          client.describeTopics(client.listTopicNames()),
          ksqlEngine.getKsqlConfig(),
          kafkaConsumerGroupClient
      );
    }
  }

  private Collection<KsqlTopic> getKsqlTopics() {
    return ksqlEngine.getMetaStore().getAllKsqlTopics().values();
  }

  private KsqlTopicsList listRegisteredTopics(String statementText) {
    return KsqlTopicsList.build(statementText, getKsqlTopics());
  }

  // Only shows queries running on the current machine, not across the entire cluster
  private Queries showQueries(String statementText) {
    List<Queries.RunningQuery> runningQueries = new ArrayList<>();
    for (PersistentQueryMetadata persistentQueryMetadata :
        ksqlEngine.getPersistentQueries().values()
    ) {
      KsqlStructuredDataOutputNode ksqlStructuredDataOutputNode =
          (KsqlStructuredDataOutputNode) persistentQueryMetadata.getOutputNode();

      runningQueries.add(new Queries.RunningQuery(
          persistentQueryMetadata.getStatementString(),
          ksqlStructuredDataOutputNode.getKafkaTopicName(),
          persistentQueryMetadata.getId()
      ));
    }
    return new Queries(statementText, runningQueries);
  }

  private TopicDescription describeTopic(String statementText, String name) throws KsqlException {
    KsqlTopic ksqlTopic = ksqlEngine.getMetaStore().getTopic(name);
    if (ksqlTopic == null) {
      throw new KsqlException(String.format(
          "Could not find Topic '%s' in the Metastore",
          name
      ));
    }
    String schemaString = null;
    TopicDescription topicDescription = new TopicDescription(
        statementText,
        name,
        ksqlTopic.getKafkaTopicName(),
        ksqlTopic
            .getKsqlTopicSerDe()
            .getSerDe()
            .toString(),
        schemaString
    );
    return topicDescription;
  }

  private SourceDescription describe(String name, boolean extended) throws KsqlException {

    StructuredDataSource dataSource = ksqlEngine.getMetaStore().getSource(name);
    if (dataSource == null) {
      throw new KsqlException(String.format(
          "Could not find STREAM/TABLE '%s' in the Metastore",
          name
      ));
    }

    List<PersistentQueryMetadata>
        queries =
        ksqlEngine
            .getPersistentQueries()
            .values()
            .stream()
            .filter(meta ->
                        ((KsqlStructuredDataOutputNode) meta.getOutputNode())
                            .getKafkaTopicName()
                            .equals(dataSource
                                        .getKsqlTopic()
                                        .getTopicName()))
            .collect(Collectors.toList());
    return new SourceDescription(
        dataSource,
        extended,
        dataSource.getKsqlTopic().getKsqlTopicSerDe().getSerDe().name(),
        "",
        "",
        getReadQueryIds(queries),
        getWriteQueryIds(queries),
        ksqlEngine.getTopicClient()
    );
  }

  private List<String> getReadQueryIds(List<PersistentQueryMetadata> queries) {
    return queries.stream().map(q -> q.getId().toString() + " : " + q.getStatementString()).collect(
        Collectors.toList());
  }

  private List<String> getWriteQueryIds(List<PersistentQueryMetadata> queries) {
    return queries
        .stream()
        .map(q -> "id:" + q.getId().toString() + " - " + q.getStatementString())
        .collect(Collectors.toList());
  }


  private PropertiesList listProperties(String statementText) {
    return new PropertiesList(statementText, ksqlEngine.getKsqlConfigProperties());
  }

  private StreamsList listStreams(String statementText) {
    return StreamsList.fromKsqlStreams(statementText, getSpecificSources(KsqlStream.class));
  }

  private TablesList listTables(String statementText) {
    return TablesList.fromKsqlTables(statementText, getSpecificSources(KsqlTable.class));
  }

  private SourceDescription getStatementExecutionPlan(Explain explain, String statementText)
      throws KsqlException {
    return getStatementExecutionPlan(
        explain.getQueryId(),
        explain.getStatement(),
        statementText,
        Collections.emptyMap()
    );
  }

  private SourceDescription getStatementExecutionPlan(
      String queryId, Statement statement, String statementText,
      Map<String, Object> properties
  ) throws KsqlException {

    if (queryId != null) {
      PersistentQueryMetadata metadata =
          ksqlEngine.getPersistentQueries().get(new QueryId(queryId));
      if (metadata == null) {
        throw new KsqlException((
                                    "Query with id:"
                                    + queryId
                                    + " does not exist, use SHOW QUERIES to view the full set of "
                                    + "queries."
                                ));
      }
      KsqlStructuredDataOutputNode outputNode =
          (KsqlStructuredDataOutputNode) metadata.getOutputNode();
      return new SourceDescription(
          outputNode,
          metadata.getStatementString(),
          metadata.getStatementString(),
          metadata.getTopologyDescription(),
          metadata.getExecutionPlan(),
          ksqlEngine.getTopicClient()
      );
    }

    DDLCommandTask ddlCommandTask = ddlCommandTasks.get(statement.getClass());
    if (ddlCommandTask != null) {
      try {
        String executionPlan = ddlCommandTask.execute(statement, statementText, properties);
        return new SourceDescription(
            "",
            "User-Evaluation",
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            "QUERY",
            "",
            "",
            "",
            "",
            true,
            "",
            "",
            "",
            executionPlan,
            0,
            0
        );
      } catch (KsqlException ksqlException) {
        throw ksqlException;
      } catch (Throwable t) {
        throw new KsqlException("Cannot RUN execution plan for this statement, " + statement, t);
      }
    }
    throw new KsqlException("Cannot FIND execution plan for this statement:" + statement);
  }

  private interface DDLCommandTask {

    String execute(Statement statement, String statementText, Map<String, Object> properties)
        throws Exception;
  }

  private Map<Class, DDLCommandTask> ddlCommandTasks = new HashMap<>();

  private void registerDdlCommandTasks() {
    ddlCommandTasks.put(
        Query.class,
        (statement, statementText, properties) -> ksqlEngine
            .getQueryExecutionPlan((Query) statement)
            .getExecutionPlan()
    );

    ddlCommandTasks.put(CreateStreamAsSelect.class, (statement, statementText, properties) -> {
      QueryMetadata
          queryMetadata =
          ksqlEngine.getQueryExecutionPlan(((CreateStreamAsSelect) statement).getQuery());
      if (queryMetadata.getDataSourceType() == DataSource.DataSourceType.KTABLE) {
        throw new KsqlException("Invalid result type. Your SELECT query produces a TABLE. Please "
                                + "use CREATE TABLE AS SELECT statement instead.");
      }
      if (queryMetadata instanceof PersistentQueryMetadata) {
        new AvroUtil().validatePersistentQueryResults(
            (PersistentQueryMetadata) queryMetadata,
            ksqlEngine.getSchemaRegistryClient()
        );
      }
      queryMetadata.close();
      return queryMetadata.getExecutionPlan();
    });

    ddlCommandTasks.put(CreateTableAsSelect.class, (statement, statementText, properties) -> {
      QueryMetadata queryMetadata =
          ksqlEngine.getQueryExecutionPlan(((CreateTableAsSelect) statement).getQuery());
      if (queryMetadata.getDataSourceType() != DataSource.DataSourceType.KTABLE) {
        throw new KsqlException("Invalid result type. Your SELECT query produces a STREAM. Please "
                                + "use CREATE STREAM AS SELECT statement instead.");
      }
      if (queryMetadata instanceof PersistentQueryMetadata) {
        new AvroUtil().validatePersistentQueryResults(
            (PersistentQueryMetadata) queryMetadata,
            ksqlEngine.getSchemaRegistryClient()
        );
      }
      queryMetadata.close();
      return queryMetadata.getExecutionPlan();
    });

    ddlCommandTasks.put(RegisterTopic.class, (statement, statementText, properties) -> {
      RegisterTopicCommand registerTopicCommand =
          new RegisterTopicCommand((RegisterTopic) statement);
      new DDLCommandExec(ksqlEngine.getMetaStore().clone()).execute(registerTopicCommand);
      return statement.toString();
    });

    ddlCommandTasks.put(CreateStream.class, (statement, statementText, properties) -> {
      CreateStreamCommand createStreamCommand =
          new CreateStreamCommand(
              statementText,
              (CreateStream) statement,
              properties,
              ksqlEngine.getTopicClient(),
              true
          );
      executeDDLCommand(createStreamCommand);
      return statement.toString();
    });

    ddlCommandTasks.put(CreateTable.class, (statement, statementText, properties) -> {
      CreateTableCommand createTableCommand =
          new CreateTableCommand(
              statementText,
              (CreateTable) statement,
              properties,
              ksqlEngine.getTopicClient(),
              true
          );
      executeDDLCommand(createTableCommand);
      return statement.toString();
    });

    ddlCommandTasks.put(DropTopic.class, (statement, statementText, properties) -> {
      DropTopicCommand dropTopicCommand = new DropTopicCommand((DropTopic) statement);
      new DDLCommandExec(ksqlEngine.getMetaStore().clone()).execute(dropTopicCommand);
      return statement.toString();
    });

    ddlCommandTasks.put(DropStream.class, (statement, statementText, properties) -> {
      DropSourceCommand dropSourceCommand = new DropSourceCommand(
          (DropStream) statement,
          DataSource.DataSourceType.KSTREAM,
          ksqlEngine
      );
      executeDDLCommand(dropSourceCommand);
      return statement.toString();
    });

    ddlCommandTasks.put(DropTable.class, (statement, statementText, properties) -> {
      DropSourceCommand dropSourceCommand = new DropSourceCommand(
          (DropTable) statement,
          DataSource.DataSourceType.KTABLE,
          ksqlEngine
      );
      executeDDLCommand(dropSourceCommand);
      return statement.toString();
    });

    ddlCommandTasks.put(
        TerminateQuery.class,
        (statement, statementText, properties) -> statement.toString()
    );
  }

  private <S extends StructuredDataSource> List<S> getSpecificSources(Class<S> dataSourceClass) {
    return ksqlEngine.getMetaStore().getAllStructuredDataSources().values().stream()
        .filter(dataSourceClass::isInstance)
        .filter(structuredDataSource -> !structuredDataSource.getName().equalsIgnoreCase(
            KsqlRestApplication.getCommandsStreamName()))
        .map(dataSourceClass::cast)
        .collect(Collectors.toList());
  }

  private void executeDDLCommand(DDLCommand ddlCommand) {
    DDLCommandResult ddlCommandResult = new DDLCommandExec(
        ksqlEngine
            .getMetaStore()
            .clone()).execute(ddlCommand);
    if (!ddlCommandResult.isSuccess()) {
      throw new KsqlException(ddlCommandResult.getMessage());
    }
  }

  private Pair<AbstractStreamCreateStatement, String> maybeAddFieldsFromSchemaRegistry(
      AbstractStreamCreateStatement streamCreateStatement,
      Map<String, Object> streamsProperties
  ) {
    Pair<AbstractStreamCreateStatement, String> avroCheckResult =
        new AvroUtil().checkAndSetAvroSchema(
            streamCreateStatement,
            streamsProperties,
            ksqlEngine.getSchemaRegistryClient()
        );
    return avroCheckResult;
  }
}
