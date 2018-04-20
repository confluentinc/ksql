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

import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.util.SchemaUtil;
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
import java.util.stream.Stream;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.CreateTableCommand;
import io.confluent.ksql.ddl.commands.DdlCommand;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.ddl.commands.DdlCommandResult;
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
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DdlStatement;
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
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
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
  public Response handleKsqlStatements(KsqlRequest request) {
    List<Statement> parsedStatements;
    List<String> statementStrings;
    Map<String, Object> streamsProperties;

    try {
      parsedStatements = ksqlEngine.getStatements(request.getKsql());
      statementStrings = getStatementStrings(request.getKsql());
    } catch (KsqlException e) {
      return Errors.badRequest(e);
    }

    streamsProperties = request.getStreamsProperties();
    if (parsedStatements.size() != statementStrings.size()) {
      return Errors.badRequest(String.format(
          "Size of parsed statements and statement strings differ; %d vs. %d, respectively",
          parsedStatements.size(),
          statementStrings.size()
      ));
    }

    KsqlEntityList result = new KsqlEntityList();
    for (int i = 0; i < parsedStatements.size(); i++) {
      String statementText = statementStrings.get(i);
      try {
        validateStatement(
            result, statementStrings.get(i), parsedStatements.get(i), streamsProperties);
      } catch (KsqlRestException e) {
        throw e;
      } catch (KsqlException e) {
        return Errors.badStatement(e, statementText, result);
      } catch (Exception e) {
        return Errors.serverErrorForStatement(e, statementText, result);
      }
      try {
        result.add(executeStatement(statementText, parsedStatements.get(i), streamsProperties));
      } catch (Exception e) {
        return Errors.serverErrorForStatement(e, statementText, result);
      }
    }
    return Response.ok(result).build();
  }

  private void validateStatement(
      final KsqlEntityList entities, final String statementText, final Statement statement,
      final Map<String, Object> streamsProperties) {
    if (statement == null) {
      throw new KsqlRestException(
          Errors.badStatement(
              String.format("Unable to execute statement '%s'", statementText),
              statementText, entities));
    }

    if (Stream.of(
        ListTopics.class, ListRegisteredTopics.class, ListStreams.class,
        ListTables.class, ListQueries.class, ListProperties.class, RunScript.class)
        .anyMatch(c -> c.isInstance(statement))) {
      return;
    }

    if (statement instanceof Query || statement instanceof PrintTopic) {
      throw new KsqlRestException(Errors.queryEndpoint(statementText, entities));
    }

    if (statement instanceof ShowColumns) {
      ShowColumns showColumns = (ShowColumns) statement;
      if (showColumns.isTopic()) {
        describeTopic(statementText, showColumns.getTable().getSuffix());
      } else {
        describe(showColumns.getTable().getSuffix(), showColumns.isExtended());
      }
    } else if (statement instanceof Explain) {
      getStatementExecutionPlan((Explain) statement, statementText);
    } else if (isExecutableDdlStatement(statement)
        || statement instanceof CreateAsSelect
        || statement instanceof TerminateQuery) {
      Statement statementWithFields = statement;
      String statementTextWithFields = statementText;
      if (statement instanceof AbstractStreamCreateStatement) {
        AbstractStreamCreateStatement streamCreateStatement = (AbstractStreamCreateStatement)
            statement;
        Pair<AbstractStreamCreateStatement, String> avroCheckResult =
            maybeAddFieldsFromSchemaRegistry(streamCreateStatement, streamsProperties);

        if (avroCheckResult.getRight() != null) {
          statementWithFields = avroCheckResult.getLeft();
          statementTextWithFields = avroCheckResult.getRight();
        }
      }
      getStatementExecutionPlan(
          null, statementWithFields, statementTextWithFields, streamsProperties);
    } else {
      throw new KsqlRestException(
          Errors.badStatement(
              String.format("Unable to execute statement '%s'", statementText),
              statementText, entities));
    }
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
      Map<String, Object> streamsProperties) {
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
    } else if (isExecutableDdlStatement(statement)
               || statement instanceof CreateAsSelect
               || statement instanceof TerminateQuery
    ) {
      return distributeStatement(statementText, statement, streamsProperties);
    }
    // This line is unreachable. Once we have distinct exception types we won't need a
    // separate validation phase for each statement and this can go away. For now all
    // exceptions are KsqlExceptions so we have to use the context to decide if its an
    // input or system error.
    throw new RuntimeException(
        "Unexpected statement of type " + statement.getClass().getSimpleName());
  }

  private boolean isExecutableDdlStatement(Statement statement) {
    return statement instanceof DdlStatement && !(statement instanceof SetProperty);
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
          client.describeTopics(client.listNonInternalTopicNames()),
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
          persistentQueryMetadata.getQueryId()
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
    return queries
        .stream()
        .map(q -> q.getQueryId().toString() + " : " + q.getStatementString())
        .collect(Collectors.toList());
  }

  private List<String> getWriteQueryIds(List<PersistentQueryMetadata> queries) {
    return queries
        .stream()
        .map(q -> "id:" + q.getQueryId().toString() + " - " + q.getStatementString())
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

  private SourceDescription getStatementExecutionPlan(Explain explain, String statementText) {
    return getStatementExecutionPlan(
        explain.getQueryId(),
        explain.getStatement(),
        statementText,
        Collections.emptyMap()
    );
  }

  private SourceDescription getStatementExecutionPlan(
      String queryId, Statement statement, String statementText,
      Map<String, Object> properties) {

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
      return new SourceDescription(
          metadata,
          ksqlEngine.getTopicClient()
      );
    }

    DdlCommandTask ddlCommandTask = ddlCommandTasks.get(statement.getClass());
    if (ddlCommandTask != null) {
      QueryMetadata queryMetadata = ddlCommandTask.execute(statement, statementText, properties);
      String executionPlan = "";
      List<SourceDescription.FieldSchemaInfo> schemaInfoList = Collections.emptyList();
      Map<String, Object> overriddenProperties = Collections.emptyMap();
      String topologyDescription = "";
      if (queryMetadata != null) {
        schemaInfoList = queryMetadata.getResultSchema().fields().stream().map(
            field -> new SourceDescription.FieldSchemaInfo(
                field.name(), SchemaUtil.getSchemaFieldName(field))
        ).collect(Collectors.toList());
        topologyDescription = queryMetadata.getTopologyDescription();
        overriddenProperties = queryMetadata.getOverriddenProperties();
        executionPlan = queryMetadata.getExecutionPlan();
      }
      return new SourceDescription(
          "",
          "User-Evaluation",
          Collections.emptyList(),
          Collections.emptyList(),
          schemaInfoList,
          "QUERY",
          "",
          "",
          "",
          "",
          true,
          "",
          "",
          topologyDescription,
          executionPlan,
          0,
          0,
          overriddenProperties
      );
    }
    throw new KsqlException("Cannot FIND execution plan for this statement:" + statement);
  }

  private interface DdlCommandTask {
    QueryMetadata execute(Statement statement, String statementText,
                          Map<String, Object> properties);
  }

  private Map<Class, DdlCommandTask> ddlCommandTasks = new HashMap<>();

  private void registerDdlCommandTasks() {
    ddlCommandTasks.put(
        Query.class,
        (statement, statementText, properties) -> {
          QueryMetadata queryMetadata = ksqlEngine.getQueryExecutionPlan((Query)statement);
          return queryMetadata;
        }
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
      return queryMetadata;
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
      return queryMetadata;
    });

    ddlCommandTasks.put(RegisterTopic.class, (statement, statementText, properties) -> {
      RegisterTopicCommand registerTopicCommand =
          new RegisterTopicCommand((RegisterTopic) statement);
      new DdlCommandExec(ksqlEngine.getMetaStore().clone()).execute(registerTopicCommand, true);
      return null;
    });

    ddlCommandTasks.put(CreateStream.class, (statement, statementText, properties) -> {
      CreateStreamCommand createStreamCommand =
          new CreateStreamCommand(
              statementText,
              (CreateStream) statement,
              ksqlEngine.getTopicClient(),
              true
          );
      executeDdlCommand(createStreamCommand);
      return null;
    });

    ddlCommandTasks.put(CreateTable.class, (statement, statementText, properties) -> {
      CreateTableCommand createTableCommand =
          new CreateTableCommand(
              statementText,
              (CreateTable) statement,
              ksqlEngine.getTopicClient(),
              true
          );
      executeDdlCommand(createTableCommand);
      return null;
    });

    ddlCommandTasks.put(DropTopic.class, (statement, statementText, properties) -> {
      DropTopicCommand dropTopicCommand = new DropTopicCommand((DropTopic) statement);
      new DdlCommandExec(ksqlEngine.getMetaStore().clone()).execute(dropTopicCommand, true);
      return null;
    });

    ddlCommandTasks.put(DropStream.class, (statement, statementText, properties) -> {
      DropSourceCommand dropSourceCommand = new DropSourceCommand(
          (DropStream) statement,
          DataSource.DataSourceType.KSTREAM,
          ksqlEngine.getSchemaRegistryClient()
      );
      executeDdlCommand(dropSourceCommand);
      return null;
    });

    ddlCommandTasks.put(DropTable.class, (statement, statementText, properties) -> {
      DropSourceCommand dropSourceCommand = new DropSourceCommand(
          (DropTable) statement,
          DataSource.DataSourceType.KTABLE,
          ksqlEngine.getSchemaRegistryClient()
      );
      executeDdlCommand(dropSourceCommand);
      return null;
    });

    ddlCommandTasks.put(
        TerminateQuery.class,
        (statement, statementText, properties) -> null
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

  private void executeDdlCommand(DdlCommand ddlCommand) {
    DdlCommandResult ddlCommandResult = new DdlCommandExec(
        ksqlEngine
            .getMetaStore()
            .clone()).execute(ddlCommand, true);
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
