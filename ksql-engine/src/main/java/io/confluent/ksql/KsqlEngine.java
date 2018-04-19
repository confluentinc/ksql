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

import com.google.common.collect.ImmutableSet;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CommandFactories;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.CreateTableCommand;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.ddl.commands.DdlCommandResult;
import io.confluent.ksql.ddl.commands.DropSourceCommand;
import io.confluent.ksql.ddl.commands.DropTopicCommand;
import io.confluent.ksql.ddl.commands.RegisterTopicCommand;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactory;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

public class KsqlEngine implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(KsqlEngine.class);

  // TODO: Decide if any other properties belong in here
  private static final Set<String> IMMUTABLE_PROPERTIES = ImmutableSet.of(
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
  );

  private final KsqlConfig ksqlConfig;
  private final MetaStore metaStore;
  private final KafkaTopicClient topicClient;
  private final DdlCommandExec ddlCommandExec;
  private final QueryEngine queryEngine;
  private final Map<QueryId, PersistentQueryMetadata> persistentQueries;
  private final Set<QueryMetadata> livePersistentQueries;
  private final Set<QueryMetadata> allLiveQueries;
  private final KsqlEngineMetrics engineMetrics;
  private final ScheduledExecutorService aggregateMetricsCollector;
  private final FunctionRegistry functionRegistry;
  private SchemaRegistryClient schemaRegistryClient;

  public KsqlEngine(final KsqlConfig ksqlConfig, final KafkaTopicClient topicClient) {
    this(
        ksqlConfig,
        topicClient,
        new KsqlSchemaRegistryClientFactory(ksqlConfig).create(),
        new MetaStoreImpl()
    );
  }

  public KsqlEngine(
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient topicClient,
      final SchemaRegistryClient schemaRegistryClient,
      final MetaStore metaStore
  ) {
    Objects.requireNonNull(ksqlConfig, "ksqlConfig can't be null");
    Objects.requireNonNull(topicClient, "topicClient can't be null");
    Objects.requireNonNull(schemaRegistryClient, "schemaRegistryClient can't be null");
    this.ksqlConfig = ksqlConfig;
    this.metaStore = metaStore;
    this.topicClient = topicClient;
    this.ddlCommandExec = new DdlCommandExec(this.metaStore);
    this.queryEngine = new QueryEngine(
        this,
        new CommandFactories(topicClient, schemaRegistryClient, true));
    this.persistentQueries = new HashMap<>();
    this.livePersistentQueries = new HashSet<>();
    this.allLiveQueries = new HashSet<>();
    this.functionRegistry = new FunctionRegistry();
    this.schemaRegistryClient = schemaRegistryClient;
    this.engineMetrics = new KsqlEngineMetrics("ksql-engine", this);
    this.aggregateMetricsCollector = Executors.newSingleThreadScheduledExecutor();
    aggregateMetricsCollector.scheduleAtFixedRate(
        engineMetrics::updateMetrics,
        1000,
        1000,
        TimeUnit.MILLISECONDS
    );
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
      final Map<String, Object> overriddenProperties
  ) throws Exception {
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
    List<Pair<String, Statement>> queries = parseQueries(
        queriesString,
        overriddenProperties,
        tempMetaStore
    );

    return planQueries(queries, overriddenProperties, tempMetaStore);
  }

  private List<QueryMetadata> planQueries(
      final List<Pair<String, Statement>> statementList,
      final Map<String, Object> overriddenProperties,
      final MetaStore tempMetaStore
  ) throws Exception {
    // Logical plan creation from the ASTs
    List<Pair<String, PlanNode>> logicalPlans = queryEngine.buildLogicalPlans(
        tempMetaStore,
        statementList
    );

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
        persistentQueries.put(persistentQueryMetadata.getQueryId(), persistentQueryMetadata);
        metaStore.updateForPersistentQuery(persistentQueryMetadata.getQueryId().getId(),
                                           persistentQueryMetadata.getSourceNames(),
                                           persistentQueryMetadata.getSinkNames());
      }
      allLiveQueries.add(queryMetadata);
    }

    return runningQueries;
  }

  public QueryMetadata getQueryExecutionPlan(final Query query) {

    // Logical plan creation from the ASTs
    List<Pair<String, PlanNode>> logicalPlans = queryEngine.buildLogicalPlans(
        metaStore,
        Collections.singletonList(new Pair<>("", query))
    );

    // Physical plan creation from logical plans.
    List<QueryMetadata> runningQueries = queryEngine.buildPhysicalPlans(
        logicalPlans,
        Collections.singletonList(new Pair<>("", query)),
        Collections.emptyMap(),
        false
    );
    return runningQueries.get(0);
  }


  // Visible for Testing
  List<Pair<String, Statement>> parseQueries(
      final String queriesString,
      final Map<String, Object> overriddenProperties,
      final MetaStore tempMetaStore
  ) {
    try {
      MetaStore tempMetaStoreForParser = tempMetaStore.clone();
      // Parse and AST creation
      KsqlParser ksqlParser = new KsqlParser();

      List<SqlBaseParser.SingleStatementContext> parsedStatements
          = ksqlParser.getStatements(queriesString);
      List<Pair<String, Statement>> queryList = new ArrayList<>();

      for (SqlBaseParser.SingleStatementContext singleStatementContext : parsedStatements) {
        Pair<Statement, DataSourceExtractor> statementInfo = ksqlParser.prepareStatement(
            singleStatementContext,
            tempMetaStoreForParser
        );
        Statement statement = statementInfo.getLeft();

        Pair<String, Statement> queryPair =
            buildSingleQueryAst(
                statement,
                getStatementString(singleStatementContext),
                tempMetaStore,
                tempMetaStoreForParser,
                overriddenProperties
            );
        if (queryPair != null) {
          queryList.add(queryPair);
        }
      }
      return queryList;
    } catch (Exception e) {
      throw new ParseFailedException("Exception while processing statements :" + e.getMessage(), e);
    }
  }

  private Pair<String, Statement> buildSingleQueryAst(
      final Statement statement,
      final String statementString,
      final MetaStore tempMetaStore,
      final MetaStore tempMetaStoreForParser,
      final Map<String, Object> overriddenProperties
  ) {

    log.info("Building AST for {}.", statementString);

    if (statement instanceof Query) {
      return new Pair<>(statementString, statement);
    } else if (statement instanceof CreateAsSelect) {
      CreateAsSelect createAsSelect = (CreateAsSelect) statement;
      QuerySpecification querySpecification =
          (QuerySpecification) createAsSelect.getQuery().getQueryBody();
      Query query = addInto(
          createAsSelect.getQuery(),
          querySpecification,
          createAsSelect.getName().getSuffix(),
          createAsSelect.getProperties(),
          createAsSelect.getPartitionByColumn()
      );
      tempMetaStoreForParser.putSource(
          queryEngine.getResultDatasource(
              querySpecification.getSelect(),
              createAsSelect.getName().getSuffix()
          ).cloneWithTimeKeyColumns());
      return new Pair<>(statementString, query);
    } else if (statement instanceof RegisterTopic) {
      ddlCommandExec.tryExecute(
          new RegisterTopicCommand(
              (RegisterTopic) statement
          ),
          tempMetaStoreForParser
      );
      ddlCommandExec.tryExecute(
          new RegisterTopicCommand(
              (RegisterTopic) statement
          ),
          tempMetaStore
      );
      return new Pair<>(statementString, statement);
    } else if (statement instanceof CreateStream) {
      ddlCommandExec.tryExecute(
          new CreateStreamCommand(
              statementString,
              (CreateStream) statement,
              topicClient,
              false),
          tempMetaStoreForParser

      );
      ddlCommandExec.tryExecute(
          new CreateStreamCommand(
              statementString,
              (CreateStream) statement,
              topicClient,
              false),
          tempMetaStore
      );
      return new Pair<>(statementString, statement);
    } else if (statement instanceof CreateTable) {
      ddlCommandExec.tryExecute(
          new CreateTableCommand(
              statementString,
              (CreateTable) statement,
              topicClient,
              false),
          tempMetaStoreForParser
      );
      ddlCommandExec.tryExecute(
          new CreateTableCommand(
              statementString,
              (CreateTable) statement,
              topicClient,
              false),
          tempMetaStore
      );
      return new Pair<>(statementString, statement);
    } else if (statement instanceof DropStream) {
      ddlCommandExec.tryExecute(new DropSourceCommand(
                                    (DropStream) statement,
                                    DataSource.DataSourceType.KSTREAM,
                                    schemaRegistryClient),
                                tempMetaStore);
      ddlCommandExec.tryExecute(new DropSourceCommand(
                                    (DropStream) statement,
                                    DataSource.DataSourceType.KSTREAM,
                                    schemaRegistryClient),
                                tempMetaStoreForParser);
      return new Pair<>(statementString, statement);
    } else if (statement instanceof DropTable) {
      ddlCommandExec.tryExecute(new DropSourceCommand(
                                    (DropTable) statement,
                                    DataSource.DataSourceType.KTABLE,
                                    schemaRegistryClient),
                                tempMetaStore);
      ddlCommandExec.tryExecute(new DropSourceCommand(
                                    (DropTable) statement,
                                    DataSource.DataSourceType.KTABLE,
                                    schemaRegistryClient),
                                tempMetaStoreForParser);
      return new Pair<>(statementString, statement);
    } else if (statement instanceof DropTopic) {
      ddlCommandExec.tryExecute(new DropTopicCommand((DropTopic) statement),
                                tempMetaStore);
      ddlCommandExec.tryExecute(
          new DropTopicCommand((DropTopic) statement),
          tempMetaStoreForParser
      );
      return new Pair<>(statementString, statement);
    } else if (statement instanceof SetProperty) {
      return new Pair<>(statementString, statement);
    }

    return null;
  }

  public static String getStatementString(
      final SqlBaseParser.SingleStatementContext singleStatementContext
  ) {
    CharStream charStream = singleStatementContext.start.getInputStream();
    return charStream.getText(new Interval(
        singleStatementContext.start.getStartIndex(),
        singleStatementContext.stop.getStopIndex()
    ));
  }

  public List<Statement> getStatements(final String sqlString) {
    return new KsqlParser().buildAst(sqlString, metaStore);
  }


  public Query addInto(
      final Query query, final QuerySpecification querySpecification,
      final String intoName,
      final Map<String, Expression> intoProperties,
      final Optional<Expression> partitionByExpression
  ) {
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
        querySpecification.getLimit()
    );
    return new Query(newQuerySpecification, query.getLimit());
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

  public DdlCommandExec getDdlCommandExec() {
    return ddlCommandExec;
  }

  public boolean terminateQuery(final QueryId queryId, final boolean closeStreams) {
    PersistentQueryMetadata persistentQueryMetadata = persistentQueries.remove(queryId);
    if (persistentQueryMetadata == null) {
      return false;
    }
    livePersistentQueries.remove(persistentQueryMetadata);
    allLiveQueries.remove(persistentQueryMetadata);
    metaStore.removePersistentQuery(persistentQueryMetadata.getQueryId().getId());
    if (closeStreams) {
      persistentQueryMetadata.close();
      persistentQueryMetadata.cleanUpInternalTopicAvroSchemas(schemaRegistryClient);
    }

    return true;
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
  public void close() {
    for (QueryMetadata queryMetadata : allLiveQueries) {
      queryMetadata.close();
    }
    topicClient.close();
    engineMetrics.close();
    aggregateMetricsCollector.shutdown();
  }

  public void removeTemporaryQuery(QueryMetadata queryMetadata) {
    this.allLiveQueries.remove(queryMetadata);
  }


  public DdlCommandResult executeDdlStatement(
      String sqlExpression, final DdlStatement statement,
      final Map<String, Object> overriddenProperties
  ) {
    return queryEngine.handleDdlStatement(sqlExpression, statement, overriddenProperties);
  }

  public SchemaRegistryClient getSchemaRegistryClient() {
    if (schemaRegistryClient != null) {
      return schemaRegistryClient;
    }
    throw new KsqlException("Cannot access the Schema Registry. Schema Registry client is null.");
  }

  public List<QueryMetadata> createQueries(final String queries) throws Exception {
    final MetaStore metaStoreCopy = metaStore.clone();
    return planQueries(
        parseQueries(
            queries,
            Collections.emptyMap(),
            metaStoreCopy
        ),
        new HashMap<>(),
        metaStoreCopy
    );
  }
}
