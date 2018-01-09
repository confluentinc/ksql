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

package io.confluent.ksql;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CommandFactories;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.CreateTableCommand;
import io.confluent.ksql.ddl.commands.DDLCommandExec;
import io.confluent.ksql.ddl.commands.DDLCommandResult;
import io.confluent.ksql.ddl.commands.DropSourceCommand;
import io.confluent.ksql.ddl.commands.DropTopicCommand;
import io.confluent.ksql.ddl.commands.RegisterTopicCommand;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DDLStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;

import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KsqlEngine implements Closeable, QueryTerminator {

  private static final Logger log = LoggerFactory.getLogger(KsqlEngine.class);

  // TODO: Decide if any other properties belong in here
  private static final Set<String> IMMUTABLE_PROPERTIES = new HashSet<>(Arrays.asList(
          StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
  ));

  private KsqlConfig ksqlConfig;

  private final MetaStore metaStore;
  private final KafkaTopicClient topicClient;
  private final DDLCommandExec ddlCommandExec;
  private final QueryEngine queryEngine;
  private final Map<QueryId, PersistentQueryMetadata> persistentQueries;
  private final Set<QueryMetadata> livePersistentQueries;
  private final Set<QueryMetadata> allLiveQueries;

  private final KsqlEngineMetrics engineMetrics;
  private final ScheduledExecutorService aggregateMetricsCollector;

  private final FunctionRegistry functionRegistry;

  private SchemaRegistryClient schemaRegistryClient;


  public KsqlEngine(final KsqlConfig ksqlConfig, final KafkaTopicClient topicClient) {

    this(ksqlConfig, topicClient, new CachedSchemaRegistryClient((String) ksqlConfig.get(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY), 1000));
  }

  public KsqlEngine(final KsqlConfig ksqlConfig, final KafkaTopicClient topicClient, SchemaRegistryClient schemaRegistryClient) {
    Objects.requireNonNull(ksqlConfig, "Streams properties map cannot be null as it may be mutated later on");
    this.ksqlConfig = ksqlConfig;
    this.metaStore = new MetaStoreImpl();
    this.topicClient = topicClient;
    this.ddlCommandExec = new DDLCommandExec(metaStore);
    this.queryEngine = new QueryEngine(this, new CommandFactories(topicClient, this));
    this.persistentQueries = new HashMap<>();
    this.livePersistentQueries = new HashSet<>();
    this.allLiveQueries = new HashSet<>();
    this.functionRegistry = new FunctionRegistry();
    this.schemaRegistryClient = schemaRegistryClient;

    this.engineMetrics = new KsqlEngineMetrics("ksql-engine", this);
    this.aggregateMetricsCollector = Executors.newSingleThreadScheduledExecutor();
    aggregateMetricsCollector.scheduleAtFixedRate(engineMetrics::updateMetrics, 1000, 1000,
                                                  TimeUnit.MILLISECONDS);
  }

  /**
   * Runs the set of queries in the given query string.
   *
   * @param queriesString The ksql query string.
   * @return List of query metadata.
   * @throws Exception Any exception thrown here!
   */
  public List<QueryMetadata> buildMultipleQueries(
      final String queriesString,
      final Map<String, Object> overriddenProperties) throws Exception {
    for (String property : overriddenProperties.keySet()) {
      if (IMMUTABLE_PROPERTIES.contains(property)) {
        throw new IllegalArgumentException(
                String.format("Cannot override property '%s'", property)
        );
      }
    }

    // Multiple queries submitted as the same time should success or fail as a whole,
    // Thus we use tempMetaStore to store newly created tables, streams or topics.
    // MetaStore tempMetaStore = new MetaStoreImpl(metaStore);

    MetaStore tempMetaStore = metaStore.clone();
    // Build query AST from the query string
    List<Pair<String, Statement>> queries = parseQueries(queriesString, overriddenProperties, tempMetaStore);

    return planQueries(queries, overriddenProperties, tempMetaStore);
  }

  public List<QueryMetadata> planQueries(final List<Pair<String, Statement>> statementList,
                                         final Map<String, Object> overriddenProperties,
                                         final MetaStore tempMetaStore) throws Exception {
    // Logical plan creation from the ASTs
    List<Pair<String, PlanNode>> logicalPlans = queryEngine.buildLogicalPlans(tempMetaStore, statementList);

    // Physical plan creation from logical plans.
    List<QueryMetadata> runningQueries = queryEngine.buildPhysicalPlans(
        logicalPlans,
        statementList,
        overriddenProperties,
        true
    );

    for (QueryMetadata queryMetadata : runningQueries) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        livePersistentQueries.add(queryMetadata);
        PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
        persistentQueries.put(persistentQueryMetadata.getId(), persistentQueryMetadata);
      }
      allLiveQueries.add(queryMetadata);
    }

    return runningQueries;
  }

  public QueryMetadata getQueryExecutionPlan(final Query query) throws Exception {

    // Logical plan creation from the ASTs
    List<Pair<String, PlanNode>> logicalPlans = queryEngine.buildLogicalPlans(metaStore,
        Collections.singletonList(new Pair<>("", query)));

    // Physical plan creation from logical plans.
    List<QueryMetadata> runningQueries = queryEngine.buildPhysicalPlans(
        logicalPlans,
        Collections.singletonList(new Pair<>("", query)),
        Collections.emptyMap(),
        false
    );
    return runningQueries.get(0);
  }


  public List<Pair<String, Statement>> parseQueries(final String queriesString,
                                                    final Map<String, Object> overriddenProperties,
                                                    final MetaStore tempMetaStore) {
    try {
      MetaStore tempMetaStoreForParser = tempMetaStore.clone();
      // Parse and AST creation
      KsqlParser ksqlParser = new KsqlParser();


      List<SqlBaseParser.SingleStatementContext> parsedStatements = ksqlParser.getStatements(queriesString);
      List<Pair<String, Statement>> queryList = new ArrayList<>();

      for (SqlBaseParser.SingleStatementContext singleStatementContext : parsedStatements) {
        Pair<Statement, DataSourceExtractor> statementInfo = ksqlParser.prepareStatement(singleStatementContext, tempMetaStoreForParser);
        Statement statement = statementInfo.getLeft();

        Pair<String, Statement> queryPair =
                buildSingleQueryAst(
                        statement,
                        getStatementString(singleStatementContext),
                        tempMetaStore,
                        tempMetaStoreForParser,
                        overriddenProperties);
        if (queryPair != null) {
          queryList.add(queryPair);
        }
      }
      return queryList;
    } catch (Exception e) {
      throw new ParseFailedException("Parsing failed on KsqlEngine msg:" + e.getMessage(), e);
    }
  }

  private Pair<String, Statement> buildSingleQueryAst(final Statement statement,
                                                      final String statementString,
                                                      final MetaStore tempMetaStore,
                                                      final MetaStore tempMetaStoreForParser,
                                                      final Map<String, Object> overriddenProperties
  ) {

    log.info("Building AST for {}.", statementString);

    if (statement instanceof Query) {
      return new Pair<>(statementString, statement);
    } else if (statement instanceof CreateStreamAsSelect) {
      CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
      QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
      Query query = addInto(
              createStreamAsSelect.getQuery(),
              querySpecification,
              createStreamAsSelect.getName().getSuffix(),
              createStreamAsSelect.getProperties(),
              createStreamAsSelect.getPartitionByColumn()
      );
      tempMetaStoreForParser.putSource(queryEngine.getResultDatasource(
              querySpecification.getSelect(),
              createStreamAsSelect.getName().getSuffix()
      ).cloneWithTimeKeyColumns());
      return new Pair<>(statementString, query);
    } else if (statement instanceof CreateTableAsSelect) {
      CreateTableAsSelect createTableAsSelect = (CreateTableAsSelect) statement;
      QuerySpecification querySpecification =
              (QuerySpecification) createTableAsSelect.getQuery().getQueryBody();

      Query query = addInto(
              createTableAsSelect.getQuery(),
              querySpecification,
              createTableAsSelect.getName().getSuffix(),
              createTableAsSelect.getProperties(),
              Optional.empty()
      );

      tempMetaStoreForParser.putSource(queryEngine.getResultDatasource(
              querySpecification.getSelect(),
              createTableAsSelect.getName().getSuffix()
      ).cloneWithTimeKeyColumns());
      return new Pair<>(statementString, query);
    } else if (statement instanceof RegisterTopic) {
      ddlCommandExec.tryExecute(
              new RegisterTopicCommand(
                      (RegisterTopic) statement,
                      overriddenProperties),
              tempMetaStoreForParser);
      ddlCommandExec.tryExecute(
              new RegisterTopicCommand(
                      (RegisterTopic) statement,
                      overriddenProperties),
              tempMetaStore);
      return new Pair<>(statementString, statement);
    } else if (statement instanceof CreateStream) {
      ddlCommandExec.tryExecute(
              new CreateStreamCommand(
                      statementString, (CreateStream) statement, overriddenProperties, topicClient),
              tempMetaStoreForParser);
      ddlCommandExec.tryExecute(
              new CreateStreamCommand(
                      statementString, (CreateStream) statement, overriddenProperties, topicClient),
              tempMetaStore);
      return new Pair<>(statementString, statement);
    } else if (statement instanceof CreateTable) {
      ddlCommandExec.tryExecute(
              new CreateTableCommand(
                      statementString, (CreateTable) statement, overriddenProperties, topicClient),
              tempMetaStoreForParser);
      ddlCommandExec.tryExecute(
              new CreateTableCommand(
                      statementString, (CreateTable) statement, overriddenProperties, topicClient),
              tempMetaStore);
      return new Pair<>(statementString, statement);
    } else if (statement instanceof DropStream) {
      ddlCommandExec.tryExecute(new DropSourceCommand((DropStream) statement, DataSource.DataSourceType.KSTREAM, this), tempMetaStore);
      ddlCommandExec.tryExecute(new DropSourceCommand((DropStream) statement, DataSource.DataSourceType.KSTREAM, this),
                                tempMetaStoreForParser);
      return new Pair<>(statementString, statement);
    } else if (statement instanceof DropTable) {
      ddlCommandExec.tryExecute(new DropSourceCommand((DropTable) statement, DataSource.DataSourceType.KTABLE, this), tempMetaStore);
      ddlCommandExec.tryExecute(new DropSourceCommand((DropTable) statement, DataSource.DataSourceType.KTABLE, this),
                                tempMetaStoreForParser);
      return new Pair<>(statementString, statement);
    } else if (statement instanceof DropTopic) {
      ddlCommandExec.tryExecute(new DropTopicCommand((DropTopic) statement), tempMetaStore);
      ddlCommandExec.tryExecute(new DropTopicCommand((DropTopic) statement), tempMetaStoreForParser);
      return new Pair<>(statementString, statement);
    } else if (statement instanceof SetProperty) {
      return new Pair<>(statementString, statement);
    }

    return null;
  }

  public static String getStatementString(
          final SqlBaseParser.SingleStatementContext singleStatementContext) {
    CharStream charStream = singleStatementContext.start.getInputStream();
    return charStream.getText(new Interval(
            singleStatementContext.start.getStartIndex(),
            singleStatementContext.stop.getStopIndex()
    ));
  }

  public List<Statement> getStatements(final String sqlString) {
    return new KsqlParser().buildAst(sqlString, metaStore);
  }


  public Query addInto(final Query query, final QuerySpecification querySpecification,
                       final String intoName,
                       final Map<String, Expression> intoProperties,
                       final Optional<Expression> partitionByExpression) {
    Table intoTable = new Table(QualifiedName.of(intoName));
    if (partitionByExpression.isPresent()) {
      Map<String, Expression> newIntoProperties = new HashMap<>();
      newIntoProperties.putAll(intoProperties);
      newIntoProperties.put(DdlConfig.PARTITION_BY_PROPERTY, partitionByExpression.get());
      intoTable.setProperties(newIntoProperties);
    } else {
      intoTable.setProperties(intoProperties);
    }

    QuerySpecification newQuerySpecification = new QuerySpecification(
            querySpecification.getSelect(),
            intoTable,
            querySpecification.getFrom(),
            querySpecification.getWindowExpression(),
            querySpecification.getWhere(),
            querySpecification.getGroupBy(),
            querySpecification.getHaving(),
            querySpecification.getOrderBy(),
            querySpecification.getLimit()
    );
    return new Query(query.getWith(), newQuerySpecification, query.getOrderBy(), query.getLimit());
  }

  public Set<QueryMetadata> getLivePersistentQueries() {
    return livePersistentQueries;
  }

  public MetaStore getMetaStore() {
    return metaStore;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  public KafkaTopicClient getTopicClient() {
    return topicClient;
  }

  public DDLCommandExec getDDLCommandExec() {
    return ddlCommandExec;
  }

  @Override
  public boolean terminateQuery(final QueryId queryId, final boolean closeStreams) {
    QueryMetadata queryMetadata = persistentQueries.remove(queryId);
    if (queryMetadata == null) {
      return false;
    }
    livePersistentQueries.remove(queryMetadata);
    allLiveQueries.remove(queryMetadata);
    if (closeStreams) {
      queryMetadata.close();
    }
    return true;
  }

  @Override
  public void terminateQueryForEntity(final String entity) {
    final Optional<PersistentQueryMetadata> query = persistentQueries.values()
        .stream()
        .filter(persistentQueryMetadata -> persistentQueryMetadata.getEntity().equalsIgnoreCase(entity))
        .findFirst();

    if (query.isPresent()) {
      final PersistentQueryMetadata metadata = query.get();
      log.info("Terminating persistent query {}", metadata.getId());
      metadata.close();
      persistentQueries.remove(metadata.getId());
      livePersistentQueries.remove(metadata);
      allLiveQueries.remove(metadata);
    }
  }

  public Map<QueryId, PersistentQueryMetadata> getPersistentQueries() {
    return new HashMap<>(persistentQueries);
  }

  public static List<String> getImmutableProperties() {
    return new ArrayList<>(IMMUTABLE_PROPERTIES);
  }

  public Map<String, Object> getKsqlConfigProperties() {
    Map<String, Object> configProperties = new HashMap<>();
    configProperties.putAll(ksqlConfig.getKsqlConfigProps());
    configProperties.putAll(ksqlConfig.getKsqlStreamConfigProps());
    return configProperties;
  }

  public KsqlConfig getKsqlConfig() {
    return ksqlConfig;
  }

  public long numberOfLiveQueries() {
    return this.allLiveQueries.size();
  }

  public long numberOfPersistentQueries() {
    return this.livePersistentQueries.size();
  }


  @Override
  public void close() throws IOException {
    for (QueryMetadata queryMetadata : livePersistentQueries) {
      queryMetadata.close();
    }
    topicClient.close();
    engineMetrics.close();
    aggregateMetricsCollector.shutdown();
  }

  @Override
  public boolean terminateAllQueries() {
    try {
      for (QueryMetadata queryMetadata : livePersistentQueries) {
        if (queryMetadata instanceof PersistentQueryMetadata) {
          PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queryMetadata;
          persistentQueryMetadata.close();
        }
        allLiveQueries.remove(queryMetadata);
      }
    } catch (Exception e) {
      return false;
    }

    return true;
  }

  public void removeTemporaryQuery(QueryMetadata queryMetadata) {
    this.allLiveQueries.remove(queryMetadata);
  }

  public DDLCommandResult executeDdlStatement(String sqlExpression, final DDLStatement statement,
                                              final Map<String, Object> streamsProperties) {
    return queryEngine.handleDdlStatement(sqlExpression, statement, streamsProperties);
  }

  public SchemaRegistryClient getSchemaRegistryClient() {
    if (schemaRegistryClient != null) {
      return schemaRegistryClient;
    }
    throw new KsqlException("Cannot access the Schema Registry. Schema Registry client is null.");
  }
}
