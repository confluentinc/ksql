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
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.exception.ParseFailedException;
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
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactory;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
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
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
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
  private final SchemaRegistryClient schemaRegistryClient;
  private final QueryIdGenerator queryIdGenerator;
  private final KafkaClientSupplier clientSupplier;
  private final AdminClient adminClient;

  private final String serviceId;

  public static KsqlEngine create(final KsqlConfig ksqlConfig) {
    final DefaultKafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();
    final AdminClient adminClient = clientSupplier
        .getAdminClient(ksqlConfig.getKsqlAdminClientConfigProps());
    return new KsqlEngine(new KafkaTopicClientImpl(adminClient),
        new KsqlSchemaRegistryClientFactory(ksqlConfig).create(),
        clientSupplier,
        new MetaStoreImpl(new InternalFunctionRegistry()),
        ksqlConfig,
        adminClient);
  }

  // called externally by tests only
  public KsqlEngine(final KafkaTopicClient topicClient,
                    final SchemaRegistryClient schemaRegistryClient,
                    final MetaStore metaStore,
                    final KsqlConfig initializationKsqlConfig
  ) {
    this(
        topicClient,
        schemaRegistryClient,
        new DefaultKafkaClientSupplier(),
        metaStore,
        initializationKsqlConfig
    );
  }

  KsqlEngine(final KafkaTopicClient kafkaTopicClient,
             final SchemaRegistryClient schemaRegistryClient,
             final KafkaClientSupplier kafkaClientSupplier,
             final MetaStore metaStore,
             final KsqlConfig initializationKsqlConfig) {
    this(kafkaTopicClient,
        schemaRegistryClient,
        kafkaClientSupplier,
        metaStore,
        initializationKsqlConfig,
        kafkaClientSupplier.getAdminClient(
            initializationKsqlConfig.getKsqlAdminClientConfigProps()));

  }

  // called externally by tests only
  KsqlEngine(final KafkaTopicClient topicClient,
             final SchemaRegistryClient schemaRegistryClient,
             final KafkaClientSupplier clientSupplier,
             final MetaStore metaStore,
             final KsqlConfig initializationKsqlConfig,
             final AdminClient adminClient
  ) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore can't be null");
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient can't be null");
    this.schemaRegistryClient =
        Objects.requireNonNull(schemaRegistryClient, "schemaRegistryClient can't be null");
    this.clientSupplier = Objects.requireNonNull(clientSupplier, "clientSupplier can't be null");
    this.serviceId = initializationKsqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
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
    this.adminClient = Objects.requireNonNull(adminClient, "adminCluent can't be null");
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
    final List<PreparedStatement> queries = parseQueries(
        queriesString,
        tempMetaStore
    );

    return planQueries(queries, ksqlConfig, overriddenProperties, tempMetaStore);
  }

  private List<QueryMetadata> planQueries(
      final List<PreparedStatement> statementList,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final MetaStore tempMetaStore
  ) {
    // Logical plan creation from the ASTs
    final List<LogicalPlanNode> logicalPlans = queryEngine.buildLogicalPlans(
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
    final List<LogicalPlanNode> logicalPlans = queryEngine.buildLogicalPlans(
        metaStore,
        Collections.singletonList(new PreparedStatement("", query)),
        ksqlConfig);

    // Physical plan creation from logical plans.
    final List<QueryMetadata> runningQueries = queryEngine.buildPhysicalPlans(
        logicalPlans,
        Collections.singletonList(new PreparedStatement("", query)),
        ksqlConfig,
        Collections.emptyMap(),
        clientSupplier,
        false
    );
    return runningQueries.get(0);
  }

  // Visible for Testing
  List<PreparedStatement> parseQueries(
      final String queriesString,
      final MetaStore tempMetaStore
  ) {
    try {
      final MetaStore tempMetaStoreForParser = tempMetaStore.clone();
      // Parse and AST creation
      final KsqlParser ksqlParser = new KsqlParser();

      final List<PreparedStatement> statements = ksqlParser.buildAst(
          queriesString,
          tempMetaStoreForParser,
          stmt -> buildSingleQueryAst(
              stmt.getStatement(), stmt.getStatementText(), tempMetaStore, tempMetaStoreForParser));

      return statements
          .stream()
          .filter(Objects::nonNull)
          .collect(Collectors.toList());

    } catch (final Exception e) {
      throw new ParseFailedException("Exception while processing statements :" + e.getMessage(), e);
    }
  }

  private PreparedStatement buildSingleQueryAst(
      final Statement statement,
      final String statementString,
      final MetaStore tempMetaStore,
      final MetaStore tempMetaStoreForParser
  ) {

    log.info("Building AST for {}.", statementString);

    if (statement instanceof Query) {
      return new PreparedStatement(statementString, statement);
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
      return new PreparedStatement(statementString, query);
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

      return new PreparedStatement(statementString, query);
    } else  if (statement instanceof DdlStatement) {
      return buildSingleDdlStatement(statement,
                                     statementString,
                                     tempMetaStore,
                                     tempMetaStoreForParser);
    }

    return null;
  }

  private PreparedStatement buildSingleDdlStatement(
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
      return new PreparedStatement(statementString, statement);
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
      return new PreparedStatement(statementString, statement);
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
      return new PreparedStatement(statementString, statement);
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
      return new PreparedStatement(statementString, statement);
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
      return new PreparedStatement(statementString, statement);
    } else if (statement instanceof DropTopic) {
      ddlCommandExec.tryExecute(new DropTopicCommand((DropTopic) statement),
                                tempMetaStore);
      ddlCommandExec.tryExecute(
          new DropTopicCommand((DropTopic) statement),
          tempMetaStoreForParser
      );
      return new PreparedStatement(statementString, statement);
    } else if (statement instanceof SetProperty) {
      return new PreparedStatement(statementString, statement);
    }
    return null;
  }

  public List<PreparedStatement> getStatements(final String sqlString) {
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

  public String getServiceId() {
    return serviceId;
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
    adminClient.close();
    engineMetrics.close();
    aggregateMetricsCollector.shutdown();
  }

  public void removeTemporaryQuery(final QueryMetadata queryMetadata) {
    this.allLiveQueries.remove(queryMetadata);
  }

  public DdlCommandResult executeDdlStatement(
      final String sqlExpression,
      final DdlStatement statement) {
    return queryEngine.handleDdlStatement(sqlExpression, statement);
  }

  public SchemaRegistryClient getSchemaRegistryClient() {
    if (schemaRegistryClient != null) {
      return schemaRegistryClient;
    }
    throw new KsqlException("Cannot access the Schema Registry. Schema Registry client is null.");
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

  public AdminClient getAdminClient() {
    return adminClient;
  }
}
