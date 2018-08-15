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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.CreateTableCommand;
import io.confluent.ksql.ddl.commands.DdlCommand;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.ddl.commands.DdlCommandResult;
import io.confluent.ksql.ddl.commands.DropSourceCommand;
import io.confluent.ksql.ddl.commands.DropTopicCommand;
import io.confluent.ksql.ddl.commands.RegisterTopicCommand;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.UdfFactory;
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
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListRegisteredTopics;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.ShowFunctions;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionInfo;
import io.confluent.ksql.rest.entity.FunctionNameList;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SimpleFunctionInfo;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.AvroUtil;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
import io.confluent.ksql.util.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.StatementWithSchema;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.slf4j.LoggerFactory;

@Path("/ksql")
@Consumes({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class KsqlResource {

  private static final org.slf4j.Logger log = LoggerFactory.getLogger(KsqlResource.class);

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final CommandStore commandStore;
  private final StatementExecutor statementExecutor;
  private final long distributedCommandResponseTimeout;

  public KsqlResource(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final CommandStore commandStore,
      final StatementExecutor statementExecutor,
      final long distributedCommandResponseTimeout
  ) {
    this.ksqlConfig = ksqlConfig;
    this.ksqlEngine = ksqlEngine;
    this.commandStore = commandStore;
    this.statementExecutor = statementExecutor;
    this.distributedCommandResponseTimeout = distributedCommandResponseTimeout;
    this.registerDdlCommandTasks();
  }

  @POST
  public Response handleKsqlStatements(final KsqlRequest request) {
    final List<Statement> parsedStatements;
    final List<String> statementStrings;
    final Map<String, Object> streamsProperties;

    try {
      parsedStatements = ksqlEngine.getStatements(request.getKsql());
      statementStrings = getStatementStrings(request.getKsql());
    } catch (final KsqlException e) {
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

    final KsqlEntityList result = new KsqlEntityList();
    for (int i = 0; i < parsedStatements.size(); i++) {
      final String statementText = statementStrings.get(i);
      try {
        validateStatement(
            result, statementStrings.get(i), parsedStatements.get(i), streamsProperties);
      } catch (final KsqlRestException e) {
        throw e;
      } catch (final KsqlException e) {
        return Errors.badStatement(e, statementText, result);
      } catch (final Exception e) {
        return Errors.serverErrorForStatement(e, statementText, result);
      }
      try {
        result.add(executeStatement(statementText, parsedStatements.get(i), streamsProperties));
      } catch (final Exception e) {
        return Errors.serverErrorForStatement(e, statementText, result);
      }
    }
    return Response.ok(result).build();
  }

  private Statement maybeAddFieldsFromSchemaRegistry(
      final Statement statement,
      final Map<String, Object> streamsProperties
  ) {
    if (statement instanceof AbstractStreamCreateStatement) {
      return AvroUtil.checkAndSetAvroSchema(
          (AbstractStreamCreateStatement)statement,
          streamsProperties,
          ksqlEngine.getSchemaRegistryClient());
    }
    return statement;
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
        ListTables.class, ListQueries.class, ListProperties.class, RunScript.class,
        ShowFunctions.class, DescribeFunction.class)
        .anyMatch(c -> c.isInstance(statement))) {
      return;
    }

    if (statement instanceof Query || statement instanceof PrintTopic) {
      throw new KsqlRestException(Errors.queryEndpoint(statementText, entities));
    }

    if (statement instanceof ShowColumns) {
      final ShowColumns showColumns = (ShowColumns) statement;
      if (showColumns.isTopic()) {
        describeTopic(statementText, showColumns.getTable().getSuffix());
      } else {
        describe(showColumns.getTable().getSuffix(), showColumns.isExtended());
      }
    } else if (statement instanceof Explain) {
      explainQuery((Explain) statement, statementText);
    } else if (isExecutableDdlStatement(statement)
        || statement instanceof CreateAsSelect
        || statement instanceof InsertInto
        || statement instanceof TerminateQuery) {
      final StatementWithSchema statementWithSchema
          = StatementWithSchema.forStatement(
              statement, statementText, streamsProperties, ksqlEngine.getSchemaRegistryClient());
      getStatementExecutionPlan(
          statementWithSchema.getStatement(),
          statementWithSchema.getStatementText(),
          streamsProperties);
    } else {
      throw new KsqlRestException(
          Errors.badStatement(
              String.format("Unable to execute statement '%s'", statementText),
              statementText, entities));
    }
  }

  public List<String> getStatementStrings(final String ksqlString) {
    final List<SqlBaseParser.SingleStatementContext> statementContexts =
        new KsqlParser().getStatements(ksqlString);
    final List<String> result = new ArrayList<>(statementContexts.size());
    for (final SqlBaseParser.SingleStatementContext statementContext : statementContexts) {
      // Taken from http://stackoverflow
      // .com/questions/16343288/how-do-i-get-the-original-text-that-an-antlr4-rule-matched
      final CharStream charStream = statementContext.start.getInputStream();
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
      final String statementText,
      final Statement statement,
      final Map<String, Object> streamsProperties) {
    if (statement instanceof ListTopics) {
      return listTopics(statementText);
    } else if (statement instanceof ListRegisteredTopics) {
      return listRegisteredTopics(statementText);
    } else if (statement instanceof ListStreams) {
      return listStreams(statementText, ((ListStreams)statement).getShowExtended());
    } else if (statement instanceof ListTables) {
      return listTables(statementText, ((ListTables)statement).getShowExtended());
    } else if (statement instanceof ListQueries) {
      return showQueries(statementText, ((ListQueries)statement).getShowExtended());
    } else if (statement instanceof ShowColumns) {
      final ShowColumns showColumns = (ShowColumns) statement;
      if (showColumns.isTopic()) {
        return describeTopic(statementText, showColumns.getTable().getSuffix());
      }
      return new SourceDescriptionEntity(
          statementText,
          describe(showColumns.getTable().getSuffix(), showColumns.isExtended()));
    } else if (statement instanceof ListProperties) {
      return listProperties(statementText, streamsProperties);
    } else if (statement instanceof Explain) {
      final Explain explain = (Explain) statement;
      return new QueryDescriptionEntity(
          statementText, explainQuery(explain, statementText));
    } else if (statement instanceof RunScript) {
      return distributeStatement(
          statementText, statement, streamsProperties, ksqlConfig);
    } else if (isExecutableDdlStatement(statement)
               || statement instanceof CreateAsSelect
               || statement instanceof InsertInto
               || statement instanceof TerminateQuery
    ) {
      final StatementWithSchema statementWithSchema
          = StatementWithSchema.forStatement(
              statement, statementText, streamsProperties, ksqlEngine.getSchemaRegistryClient());
      return distributeStatement(
          statementWithSchema.getStatementText(),
          statementWithSchema.getStatement(),
          streamsProperties,
          ksqlConfig);
    } else if (statement instanceof ShowFunctions) {
      return listFunctions(statementText);
    } else if (statement instanceof DescribeFunction) {
      return describeFunction(statementText, ((DescribeFunction)statement).getFunctionName());
    }
    // This line is unreachable. Once we have distinct exception types we won't need a
    // separate validation phase for each statement and this can go away. For now all
    // exceptions are KsqlExceptions so we have to use the context to decide if its an
    // input or system error.
    throw new RuntimeException(
        "Unexpected statement of type " + statement.getClass().getSimpleName());
  }



  private boolean isExecutableDdlStatement(final Statement statement) {
    return statement instanceof DdlStatement && !(statement instanceof SetProperty);
  }

  private CommandStatusEntity distributeStatement(
      final String statementText,
      final Statement statement,
      final Map<String, Object> propertyOverrides,
      final KsqlConfig ksqlConfig
  ) throws KsqlException {
    final CommandId commandId =
        commandStore.distributeStatement(
            statementText,
            statement,
            ksqlConfig,
            propertyOverrides);
    CommandStatus commandStatus;
    try {
      commandStatus = statementExecutor.registerQueuedStatement(commandId)
          .get(distributedCommandResponseTimeout, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException exception) {
      log.warn(
          "Timeout to get commandStatus, waited {} milliseconds:, statementText:" + statementText,
          distributedCommandResponseTimeout, exception
      );
      commandStatus = statementExecutor.getStatus(commandId).get();
    } catch (final Exception e) {
      throw new KsqlException(
          String.format(
              "Could not write the statement '%s' into the command " + "topic.", statementText
          ),
          e
      );
    }
    return new CommandStatusEntity(statementText, commandId, commandStatus);
  }

  private KafkaTopicsList listTopics(final String statementText) {
    final KafkaTopicClient client = ksqlEngine.getTopicClient();
    try (KafkaConsumerGroupClient kafkaConsumerGroupClient
             = new KafkaConsumerGroupClientImpl(ksqlConfig)) {
      return KafkaTopicsList.build(
          statementText,
          getKsqlTopics(),
          client.describeTopics(client.listNonInternalTopicNames()),
          ksqlConfig,
          kafkaConsumerGroupClient
      );
    }
  }

  private Collection<KsqlTopic> getKsqlTopics() {
    return ksqlEngine.getMetaStore().getAllKsqlTopics().values();
  }

  private KsqlTopicsList listRegisteredTopics(final String statementText) {
    return KsqlTopicsList.build(statementText, getKsqlTopics());
  }

  // Only shows queries running on the current machine, not across the entire cluster
  private KsqlEntity showQueries(final String statementText, final boolean descriptions) {
    if (descriptions) {
      return new QueryDescriptionList(
          statementText,
          ksqlEngine.getPersistentQueries().stream()
              .map(QueryDescription::forQueryMetadata)
              .collect(Collectors.toList()));
    }
    return new Queries(
        statementText,
        ksqlEngine.getPersistentQueries().stream()
            .map(
                q -> new RunningQuery(
                    q.getStatementString(),
                    q.getSinkNames(),
                    new EntityQueryId(q.getQueryId())))
            .collect(Collectors.toList()));
  }

  private TopicDescription describeTopic(
      final String statementText,
      final String name) throws KsqlException {
    final KsqlTopic ksqlTopic = ksqlEngine.getMetaStore().getTopic(name);
    if (ksqlTopic == null) {
      throw new KsqlException(String.format(
          "Could not find Topic '%s' in the Metastore",
          name
      ));
    }
    final String schemaString = null;
    final TopicDescription topicDescription = new TopicDescription(
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

  private SourceDescription describe(
      final String name,
      final boolean extended) throws KsqlException {

    final StructuredDataSource dataSource = ksqlEngine.getMetaStore().getSource(name);
    if (dataSource == null) {
      throw new KsqlException(String.format(
          "Could not find STREAM/TABLE '%s' in the Metastore",
          name
      ));
    }

    return new SourceDescription(
        dataSource,
        extended,
        dataSource.getKsqlTopic().getKsqlTopicSerDe().getSerDe().name(),
        getQueries(q -> q.getSourceNames().contains(dataSource.getName())),
        getQueries(q -> q.getSinkNames().contains(dataSource.getName())),
        ksqlEngine.getTopicClient()
    );
  }

  private List<RunningQuery> getQueries(final Predicate<PersistentQueryMetadata> predicate) {
    return ksqlEngine.getPersistentQueries()
        .stream()
        .filter(predicate)
        .map(q -> new RunningQuery(
            q.getStatementString(), q.getSinkNames(), new EntityQueryId(q.getQueryId())))
        .collect(Collectors.toList());
  }

  private ImmutableMap<String, Object> getAllProps(final KsqlConfig ksqlConfig) {
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.putAll(ksqlConfig.values().entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue))
    );
    builder.putAll(ksqlConfig.getKsqlStreamConfigProps().entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(
            Collectors.toMap(
                e -> KsqlConfig.KSQL_STREAMS_PREFIX + e.getKey(),
                Map.Entry::getValue)));
    return builder.build();
  }


  private PropertiesList listProperties(final String statementText,
                                        final Map<String, Object> overwriteProperties) {
    final Map<String, String> engineProperties
        = ksqlConfig.getAllConfigPropsWithSecretsObfuscated();
    final Map<String, String> mergedProperties
        = ksqlConfig.cloneWithPropertyOverwrite(overwriteProperties)
            .getAllConfigPropsWithSecretsObfuscated();
    final List<String> overwritten = mergedProperties.keySet()
        .stream()
        .filter(k -> !Objects.equals(engineProperties.get(k), mergedProperties.get(k)))
        .collect(Collectors.toList());
    return new PropertiesList(statementText, mergedProperties, overwritten);
  }

  private KsqlEntity listStreams(final String statementText, final boolean showDescriptions) {
    final List<KsqlStream> ksqlStreams = getSpecificSources(KsqlStream.class);
    if (showDescriptions) {
      return new SourceDescriptionList(
          statementText,
          ksqlStreams.stream()
              .map(s -> describe(s.getName(), true))
              .collect(Collectors.toList()));
    }
    return new StreamsList(
        statementText,
        ksqlStreams.stream()
            .map(SourceInfo.Stream::new)
            .collect(Collectors.toList()));
  }

  private KsqlEntity listTables(final String statementText, final boolean showDescriptions) {
    final List<KsqlTable> ksqlTables = getSpecificSources(KsqlTable.class);
    if (showDescriptions) {
      return new SourceDescriptionList(
          statementText,
          ksqlTables.stream()
              .map(t -> describe(t.getName(), true))
              .collect(Collectors.toList()));
    }
    return new TablesList(
        statementText,
        ksqlTables.stream()
            .map(SourceInfo.Table::new)
            .collect(Collectors.toList()));
  }

  private KsqlEntity listFunctions(final String statementText) {
    final List<SimpleFunctionInfo> all = ksqlEngine.listScalarFunctions()
        .stream()
        .map(factory -> new SimpleFunctionInfo(factory.getName().toUpperCase(),
            FunctionType.scalar)).collect(Collectors.toList());
    all.addAll(ksqlEngine.listAggregateFunctions()
        .stream()
        .map(factory -> new SimpleFunctionInfo(factory.getName().toUpperCase(),
            FunctionType.aggregate))
        .collect(Collectors.toList()));

    return new FunctionNameList(statementText, all);
  }

  private FunctionDescriptionList describeFunction(final String statementText,
                                                   final String functionName) {
    final ImmutableList.Builder<FunctionInfo> listBuilder = ImmutableList.builder();
    final FunctionRegistry functionRegistry = ksqlEngine.getFunctionRegistry();
    if (functionRegistry.isAggregate(functionName)) {
      final AggregateFunctionFactory aggregateFactory
          = functionRegistry.getAggregateFactory(functionName);
      aggregateFactory.eachFunction(function ->
          listBuilder.add(new FunctionInfo(function.getArgTypes()
              .stream()
              .map(SchemaUtil::getSqlTypeName).collect(Collectors.toList()),
              SchemaUtil.getSqlTypeName(function.getReturnType()),
              function.getDescription())));

      return new FunctionDescriptionList(statementText,
          aggregateFactory.getName().toUpperCase(),
          aggregateFactory.getDescription(),
          aggregateFactory.getAuthor(),
          aggregateFactory.getVersion(),
          aggregateFactory.getPath(),
          listBuilder.build(),
          FunctionType.aggregate
      );
    }

    final UdfFactory udfFactory = ksqlEngine.getFunctionRegistry().getUdfFactory(functionName);
    udfFactory.eachFunction(function ->
        listBuilder.add(new FunctionInfo(function.getArguments()
            .stream()
            .map(SchemaUtil::getSqlTypeName).collect(Collectors.toList()),
            SchemaUtil.getSqlTypeName(function.getReturnType()),
            function.getDescription())));

    return new FunctionDescriptionList(statementText,
        udfFactory.getName().toUpperCase(),
        udfFactory.getDescription(),
        udfFactory.getAuthor(),
        udfFactory.getVersion(),
        udfFactory.getPath(),
        listBuilder.build(),
        FunctionType.scalar
    );
  }

  private QueryDescription explainQuery(final Explain explain, final String statementText) {
    final String queryId = explain.getQueryId();
    if (queryId != null) {
      final PersistentQueryMetadata metadata =
          ksqlEngine.getPersistentQuery(new QueryId(queryId));
      if (metadata == null) {
        throw new KsqlException(
            "Query with id:" + queryId + " does not exist, use SHOW QUERIES to view the full "
                + "set of queries.");
      }
      return QueryDescription.forQueryMetadata(metadata);
    }
    final QueryMetadata queryMetadata = getStatementExecutionPlan(
        explain.getStatement(),
        statementText,
        Collections.emptyMap()
    );
    if (queryMetadata == null) {
      throw new KsqlException("The provided statement does not run a ksql query");
    }
    return QueryDescription.forQueryMetadata(queryMetadata);
  }

  private QueryMetadata getStatementExecutionPlan(
      final Statement statement, final String statementText, final Map<String, Object> properties) {

    final DdlCommandTask ddlCommandTask = ddlCommandTasks.get(statement.getClass());
    if (ddlCommandTask == null) {
      throw new KsqlException("Cannot FIND execution plan for this statement:" + statement);
    }
    return ddlCommandTask.execute(statement, statementText, properties);
  }

  private interface DdlCommandTask {
    QueryMetadata execute(Statement statement, String statementText,
                          Map<String, Object> properties);
  }

  private Map<Class, DdlCommandTask> ddlCommandTasks = new HashMap<>();

  private void registerDdlCommandTasks() {
    ddlCommandTasks.put(Query.class,
        (statement, statementText, properties) ->
            ksqlEngine.getQueryExecutionPlan((Query)statement, ksqlConfig)
    );

    ddlCommandTasks.put(CreateStreamAsSelect.class, (statement, statementText, properties) -> {
      final QueryMetadata
          queryMetadata =
          ksqlEngine.getQueryExecutionPlan(
              ((CreateStreamAsSelect) statement).getQuery(),
              ksqlConfig);
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
      final QueryMetadata queryMetadata =
          ksqlEngine.getQueryExecutionPlan(
              ((CreateTableAsSelect) statement).getQuery(),
              ksqlConfig);
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

    ddlCommandTasks.put(InsertInto.class, (statement, statementText, properties) -> {
      final QueryMetadata queryMetadata =
          ksqlEngine.getQueryExecutionPlan(((InsertInto) statement).getQuery(), ksqlConfig);
      if (queryMetadata instanceof PersistentQueryMetadata) {
        new AvroUtil().validatePersistentQueryResults((PersistentQueryMetadata) queryMetadata,
                                                      ksqlEngine.getSchemaRegistryClient());
      }
      queryMetadata.close();
      return queryMetadata;
    });

    ddlCommandTasks.put(RegisterTopic.class, (statement, statementText, properties) -> {
      final RegisterTopicCommand registerTopicCommand =
          new RegisterTopicCommand((RegisterTopic) statement);
      new DdlCommandExec(ksqlEngine.getMetaStore().clone()).execute(registerTopicCommand, true);
      return null;
    });

    ddlCommandTasks.put(CreateStream.class, (statement, statementText, properties) -> {
      final CreateStreamCommand createStreamCommand =
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
      final CreateTableCommand createTableCommand =
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
      final DropTopicCommand dropTopicCommand = new DropTopicCommand((DropTopic) statement);
      new DdlCommandExec(ksqlEngine.getMetaStore().clone()).execute(dropTopicCommand, true);
      return null;
    });

    ddlCommandTasks.put(DropStream.class, (statement, statementText, properties) -> {
      final DropStream dropStream = (DropStream) statement;
      final DropSourceCommand dropSourceCommand = new DropSourceCommand(
          dropStream,
          DataSource.DataSourceType.KSTREAM,
          ksqlEngine.getTopicClient(),
          ksqlEngine.getSchemaRegistryClient(),
          dropStream.isDeleteTopic()
      );
      executeDdlCommand(dropSourceCommand);
      return null;
    });

    ddlCommandTasks.put(DropTable.class, (statement, statementText, properties) -> {
      final DropTable dropTable = (DropTable) statement;
      final DropSourceCommand dropSourceCommand = new DropSourceCommand(
          dropTable,
          DataSource.DataSourceType.KTABLE,
          ksqlEngine.getTopicClient(),
          ksqlEngine.getSchemaRegistryClient(),
          dropTable.isDeleteTopic()
      );
      executeDdlCommand(dropSourceCommand);
      return null;
    });

    ddlCommandTasks.put(
        TerminateQuery.class,
        (statement, statementText, properties) -> null
    );
  }

  private <S extends StructuredDataSource> List<S> getSpecificSources(
      final Class<S> dataSourceClass) {
    return ksqlEngine.getMetaStore().getAllStructuredDataSources().values().stream()
        .filter(dataSourceClass::isInstance)
        .filter(structuredDataSource -> !structuredDataSource.getName().equalsIgnoreCase(
            KsqlRestApplication.getCommandsStreamName()))
        .map(dataSourceClass::cast)
        .collect(Collectors.toList());
  }

  private void executeDdlCommand(final DdlCommand ddlCommand) {
    final DdlCommandResult ddlCommandResult = new DdlCommandExec(
        ksqlEngine
            .getMetaStore()
            .clone()).execute(ddlCommand, true);
    if (!ddlCommandResult.isSuccess()) {
      throw new KsqlException(ddlCommandResult.getMessage());
    }
  }
}
