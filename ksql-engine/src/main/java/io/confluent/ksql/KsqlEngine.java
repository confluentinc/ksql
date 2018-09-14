/*
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
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.rewrite.StatementRewriteForStruct;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.InsertInto;
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
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.function.Supplier;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KsqlEngine implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(KsqlEngine.class);

  // TODO: Decide if any other properties belong in here
  private static final Set<String> IMMUTABLE_PROPERTIES = ImmutableSet.of(
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
  );

  private final MetaStore metaStore;
  private final KafkaTopicClient topicClient;
  private final DdlCommandExec ddlCommandExec;
  private final QueryEngine queryEngine;
  private final Map<QueryId, PersistentQueryMetadata> persistentQueries;
  private final Set<QueryMetadata> livePersistentQueries;
  private final Set<QueryMetadata> allLiveQueries;
  private final KsqlEngineMetrics engineMetrics;
  private final ScheduledExecutorService aggregateMetricsCollector;
  private final Supplier<SchemaRegistryClient> schemaRegistryClientFactory;
  private final SchemaRegistryClient schemaRegistryClient;
  private final QueryIdGenerator queryIdGenerator;
  private final KafkaClientSupplier clientSupplier;

  public KsqlEngine(final KsqlConfig ksqlConfig) {
    this(
        new KafkaTopicClientImpl(ksqlConfig.getKsqlAdminClientConfigProps()),
        (new KsqlSchemaRegistryClientFactory(ksqlConfig))::get,
        new DefaultKafkaClientSupplier(),
        new MetaStoreImpl(new InternalFunctionRegistry())
    );
  }

  public KsqlEngine(final KafkaTopicClient kafkaTopicClient,
                    final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
                    final KafkaClientSupplier clientSupplier
  ) {
    this(
        kafkaTopicClient,
        schemaRegistryClientFactory,
        clientSupplier,
        new MetaStoreImpl(new InternalFunctionRegistry())
    );
  }

  // called externally by tests only
  public KsqlEngine(final KafkaTopicClient topicClient,
                    final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
                    final MetaStore metaStore
  ) {
    this(
        topicClient,
        schemaRegistryClientFactory,
        new DefaultKafkaClientSupplier(),
        metaStore
    );
  }

  // called externally by tests only
  KsqlEngine(final KafkaTopicClient topicClient,
             final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
             final KafkaClientSupplier clientSupplier,
             final MetaStore metaStore
  ) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore can't be null");
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient can't be null");
    this.schemaRegistryClientFactory =
        Objects.requireNonNull(
            schemaRegistryClientFactory, "schemaRegistryClientFactory can't be null");
    this.schemaRegistryClient =
        Objects.requireNonNull(
            this.schemaRegistryClientFactory.get(), "Schema registry can't be null");
    this.clientSupplier = Objects.requireNonNull(clientSupplier, "clientSupplier can't be null");
    this.ddlCommandExec = new DdlCommandExec(this.metaStore);
    this.queryEngine = new QueryEngine(
        this,
        new CommandFactories(topicClient, schemaRegistryClient, true));
    this.persistentQueries = new HashMap<>();
    this.livePersistentQueries = new HashSet<>();
    this.allLiveQueries = new HashSet<>();
    this.engineMetrics = new KsqlEngineMetrics("ksql-engine", this);
    this.aggregateMetricsCollector = Executors.newSingleThreadScheduledExecutor();
    this.queryIdGenerator = new QueryIdGenerator();
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
   * @param overriddenProperties The user-requested property overrides
   * @return List of query metadata.
   */
  public List<QueryMetadata> buildMultipleQueries(
      final String queriesString,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties) {
    for (final String property : overriddenProperties.keySet()) {
      if (IMMUTABLE_PROPERTIES.contains(property)) {
        throw new IllegalArgumentException(
            String.format("Cannot override property '%s'", property)
        );
      }
    }

    // Multiple queries submitted as the same time should success or fail as a whole,
    // Thus we use tempMetaStore to store newly created tables, streams or topics.
    // MetaStore tempMetaStore = new MetaStoreImpl(metaStore);
    final MetaStore tempMetaStore = metaStore.clone();

    // Build query AST from the query string
    final List<Pair<String, Statement>> queries = parseQueries(
        queriesString,
        tempMetaStore
    );

    return planQueries(queries, ksqlConfig, overriddenProperties, tempMetaStore);
  }

  private List<QueryMetadata> planQueries(
      final List<Pair<String, Statement>> statementList,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final MetaStore tempMetaStore
  ) {
    // Logical plan creation from the ASTs
    final List<Pair<String, PlanNode>> logicalPlans = queryEngine.buildLogicalPlans(
        tempMetaStore,
        statementList,
        ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties)
    );

    // Physical plan creation from logical plans.
    final List<QueryMetadata> runningQueries = queryEngine.buildPhysicalPlans(
        logicalPlans,
        statementList,
        ksqlConfig,
        overriddenProperties,
        clientSupplier,
        true
    );

    for (final QueryMetadata queryMetadata : runningQueries) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        livePersistentQueries.add(queryMetadata);
        final PersistentQueryMetadata persistentQueryMd = (PersistentQueryMetadata) queryMetadata;
        persistentQueries.put(persistentQueryMd.getQueryId(), persistentQueryMd);
        metaStore.updateForPersistentQuery(persistentQueryMd.getQueryId().getId(),
                                           persistentQueryMd.getSourceNames(),
                                           persistentQueryMd.getSinkNames());
      }
      allLiveQueries.add(queryMetadata);
    }

    return runningQueries;
  }

  public QueryMetadata getQueryExecutionPlan(final Query query, final KsqlConfig ksqlConfig) {

    // Logical plan creation from the ASTs
    final List<Pair<String, PlanNode>> logicalPlans = queryEngine.buildLogicalPlans(
        metaStore,
        Collections.singletonList(new Pair<>("", query)),
        ksqlConfig);

    // Physical plan creation from logical plans.
    final List<QueryMetadata> runningQueries = queryEngine.buildPhysicalPlans(
        logicalPlans,
        Collections.singletonList(new Pair<>("", query)),
        ksqlConfig,
        Collections.emptyMap(),
        clientSupplier,
        false
    );
    return runningQueries.get(0);
  }

  // Visible for Testing
  List<Pair<String, Statement>> parseQueries(
      final String queriesString,
      final MetaStore tempMetaStore
  ) {
    try {
      final MetaStore tempMetaStoreForParser = tempMetaStore.clone();
      // Parse and AST creation
      final KsqlParser ksqlParser = new KsqlParser();

      final List<SqlBaseParser.SingleStatementContext> parsedStatements
          = ksqlParser.getStatements(queriesString);
      final List<Pair<String, Statement>> queryList = new ArrayList<>();

      for (final SqlBaseParser.SingleStatementContext singleStatementContext : parsedStatements) {
        final Pair<Statement, DataSourceExtractor> statementInfo = ksqlParser.prepareStatement(
            singleStatementContext,
            tempMetaStoreForParser
        );
        Statement statement = statementInfo.getLeft();
        if (StatementRewriteForStruct.requiresRewrite(statement)) {
          statement = new StatementRewriteForStruct(
              statement,
              statementInfo.getRight())
              .rewriteForStruct();
        }
        final Pair<String, Statement> queryPair =
            buildSingleQueryAst(
                statement,
                getStatementString(singleStatementContext),
                tempMetaStore,
                tempMetaStoreForParser
            );
        if (queryPair != null) {
          queryList.add(queryPair);
        }
      }
      return queryList;
    } catch (final Exception e) {
      throw new ParseFailedException("Exception while processing statements :" + e.getMessage(), e);
    }
  }

  private Pair<String, Statement> buildSingleQueryAst(
      final Statement statement,
      final String statementString,
      final MetaStore tempMetaStore,
      final MetaStore tempMetaStoreForParser
  ) {

    log.info("Building AST for {}.", statementString);

    if (statement instanceof Query) {
      return new Pair<>(statementString, statement);
    } else if (statement instanceof CreateAsSelect) {
      final CreateAsSelect createAsSelect = (CreateAsSelect) statement;
      final QuerySpecification querySpecification =
          (QuerySpecification) createAsSelect.getQuery().getQueryBody();
      final Query query = addInto(
          createAsSelect.getQuery(),
          querySpecification,
          createAsSelect.getName().getSuffix(),
          createAsSelect.getProperties(),
          createAsSelect.getPartitionByColumn(),
          true
      );
      tempMetaStoreForParser.putSource(
          queryEngine.getResultDatasource(
              querySpecification.getSelect(),
              createAsSelect.getName().getSuffix()
          ).cloneWithTimeKeyColumns());
      return new Pair<>(statementString, query);
    } else if (statement instanceof InsertInto) {
      final InsertInto insertInto = (InsertInto) statement;
      if (tempMetaStoreForParser.getSource(insertInto.getTarget().getSuffix()) == null) {
        throw new KsqlException(String.format("Sink, %s, does not exist for the INSERT INTO "
                                              + "statement.", insertInto.getTarget().getSuffix()));
      }

      if (tempMetaStoreForParser.getSource(insertInto.getTarget().getSuffix()).getDataSourceType()
          != DataSource.DataSourceType.KSTREAM) {
        throw new KsqlException(String.format("INSERT INTO can only be used to insert into a "
                                              + "stream. %s is a table.",
                                              insertInto.getTarget().getSuffix()));
      }

      final QuerySpecification querySpecification =
          (QuerySpecification) insertInto.getQuery().getQueryBody();

      final Query query = addInto(
          insertInto.getQuery(),
          querySpecification,
          insertInto.getTarget().getSuffix(),
          new HashMap<>(),
          insertInto.getPartitionByColumn(),
          false
      );

      return new Pair<>(statementString, query);
    } else  if (statement instanceof DdlStatement) {
      return buildSingleDdlStatement(statement,
                                     statementString,
                                     tempMetaStore,
                                     tempMetaStoreForParser);
    }

    return null;
  }

  private Pair<String, Statement> buildSingleDdlStatement(
      final Statement statement,
      final String statementString,
      final MetaStore tempMetaStore,
      final MetaStore tempMetaStoreForParser
  ) {
    if (statement instanceof RegisterTopic) {
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
      final DropStream dropStream = (DropStream) statement;
      ddlCommandExec.tryExecute(new DropSourceCommand(
                                    dropStream,
                                    DataSource.DataSourceType.KSTREAM,
                                    topicClient,
                                    schemaRegistryClient,
                                    dropStream.isDeleteTopic()),
                                tempMetaStore);
      ddlCommandExec.tryExecute(new DropSourceCommand(
                                    dropStream,
                                    DataSource.DataSourceType.KSTREAM,
                                    topicClient,
                                    schemaRegistryClient,
                                    dropStream.isDeleteTopic()),
                                tempMetaStoreForParser);
      return new Pair<>(statementString, statement);
    } else if (statement instanceof DropTable) {
      final DropTable dropTable = (DropTable) statement;
      ddlCommandExec.tryExecute(new DropSourceCommand(
                                    dropTable,
                                    DataSource.DataSourceType.KTABLE,
                                    topicClient,
                                    schemaRegistryClient,
                                    dropTable.isDeleteTopic()),
                                tempMetaStore);
      ddlCommandExec.tryExecute(new DropSourceCommand(
                                    dropTable,
                                    DataSource.DataSourceType.KTABLE,
                                    topicClient,
                                    schemaRegistryClient,
                                    dropTable.isDeleteTopic()),
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
    final CharStream charStream = singleStatementContext.start.getInputStream();
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
                       final Optional<Expression> partitionByExpression,
                       final boolean doCreateTable) {
    final Table intoTable = new Table(QualifiedName.of(intoName));
    if (partitionByExpression.isPresent()) {
      final Map<String, Expression> newIntoProperties = new HashMap<>(intoProperties);
      newIntoProperties.put(DdlConfig.PARTITION_BY_PROPERTY, partitionByExpression.get());
      intoTable.setProperties(newIntoProperties);
    } else {
      intoTable.setProperties(intoProperties);
    }

    final QuerySpecification newQuerySpecification = new QuerySpecification(
        querySpecification.getSelect(),
        intoTable,
        doCreateTable,
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
    return metaStore;
  }

  public KafkaTopicClient getTopicClient() {
    return topicClient;
  }

  public DdlCommandExec getDdlCommandExec() {
    return ddlCommandExec;
  }

  public boolean terminateQuery(final QueryId queryId, final boolean closeStreams) {
    final PersistentQueryMetadata persistentQueryMetadata = persistentQueries.remove(queryId);
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

  public PersistentQueryMetadata getPersistentQuery(final QueryId queryId) {
    return persistentQueries.get(queryId);
  }

  public Collection<PersistentQueryMetadata> getPersistentQueries() {
    return Collections.unmodifiableList(
        new ArrayList<>(
            persistentQueries.values()));
  }

  public static List<String> getImmutableProperties() {
    return new ArrayList<>(IMMUTABLE_PROPERTIES);
  }

  public long numberOfLiveQueries() {
    return this.allLiveQueries.size();
  }

  public long numberOfPersistentQueries() {
    return this.livePersistentQueries.size();
  }


  @Override
  public void close() {
    final Set<QueryMetadata> queriesToClose = new HashSet<>(allLiveQueries);
    for (final QueryMetadata queryMetadata : queriesToClose) {
      queryMetadata.close();
    }
    topicClient.close();
    engineMetrics.close();
    aggregateMetricsCollector.shutdown();
  }

  public void removeTemporaryQuery(final QueryMetadata queryMetadata) {
    this.allLiveQueries.remove(queryMetadata);
  }

  public DdlCommandResult executeDdlStatement(
      final String sqlExpression,
      final DdlStatement statement,
      final Map<String, Object> overriddenProperties) {
    return queryEngine.handleDdlStatement(sqlExpression, statement, overriddenProperties);
  }

  public Supplier<SchemaRegistryClient> getSchemaRegistryClientFactory() {
    return schemaRegistryClientFactory;
  }

  public SchemaRegistryClient getSchemaRegistryClient() {
    return schemaRegistryClient;
  }

  public QueryIdGenerator getQueryIdGenerator() {
    return queryIdGenerator;
  }

  public List<QueryMetadata> createQueries(final String queries, final KsqlConfig ksqlConfig) {
    final MetaStore metaStoreCopy = metaStore.clone();
    return planQueries(
        parseQueries(
            queries,
            metaStoreCopy
        ),
        ksqlConfig,
        Collections.emptyMap(),
        metaStoreCopy
    );
  }

  public List<UdfFactory> listScalarFunctions() {
    return metaStore.listFunctions();
  }

  public List<AggregateFunctionFactory> listAggregateFunctions() {
    return metaStore.listAggregateFunctions();
  }
}
