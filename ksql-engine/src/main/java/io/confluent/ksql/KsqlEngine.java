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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CommandFactories;
import io.confluent.ksql.ddl.commands.DdlCommand;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.ddl.commands.DdlCommandResult;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactory;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.util.AvroUtil;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.StatementWithSchema;
import io.confluent.ksql.util.StringUtil;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KsqlEngine implements Closeable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(KsqlEngine.class);

  private static final Set<String> IMMUTABLE_PROPERTIES = ImmutableSet.<String>builder()
      .add(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)
      .add(KsqlConfig.KSQL_EXT_DIR)
      .add(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG)
      .addAll(KsqlConfig.SSL_CONFIG_NAMES)
      .build();

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
  private final KafkaClientSupplier clientSupplier;
  private final AdminClient adminClient;

  private final String serviceId;
  private final CommandFactories ddlCommandFactory;

  public static KsqlEngine create(final KsqlConfig ksqlConfig) {
    final DefaultKafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();
    final AdminClient adminClient = clientSupplier
        .getAdminClient(ksqlConfig.getKsqlAdminClientConfigProps());
    return new KsqlEngine(
        new KafkaTopicClientImpl(adminClient),
        new KsqlSchemaRegistryClientFactory(ksqlConfig)::get,
        clientSupplier,
        new MetaStoreImpl(new InternalFunctionRegistry()),
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
        adminClient,
        KsqlEngineMetrics::new
    );
  }

  KsqlEngine(final KafkaTopicClient topicClient,
             final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
             final KafkaClientSupplier clientSupplier,
             final MetaStore metaStore,
             final String serviceId,
             final AdminClient adminClient,
             final Function<KsqlEngine, KsqlEngineMetrics> engineMetricsFactory
  ) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore can't be null");
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient can't be null");
    this.schemaRegistryClient = Objects.requireNonNull(Objects.requireNonNull(
        schemaRegistryClientFactory, "schemaRegistryClientFactory can't be null").get(),
        "Schema registry can't be null");
    this.clientSupplier = Objects.requireNonNull(clientSupplier, "clientSupplier can't be null");
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
    this.ddlCommandExec = new DdlCommandExec(this.metaStore);
    this.ddlCommandFactory = new CommandFactories(topicClient, schemaRegistryClient);
    this.queryEngine = new QueryEngine(topicClient, schemaRegistryClientFactory);
    this.persistentQueries = new HashMap<>();
    this.livePersistentQueries = new HashSet<>();
    this.allLiveQueries = new HashSet<>();
    this.engineMetrics = engineMetricsFactory.apply(this);
    this.aggregateMetricsCollector = Executors.newSingleThreadScheduledExecutor();
    this.adminClient = Objects.requireNonNull(adminClient, "adminCluent can't be null");
    aggregateMetricsCollector.scheduleAtFixedRate(
        () -> {
          try {
            this.engineMetrics.updateMetrics();
          } catch (final Exception e) {
            log.info("Error updating engine metrics", e);
          }
        },
        1000,
        1000,
        TimeUnit.MILLISECONDS
    );
  }

  /**
   * Parse the statements, but do NOT update the metastore.
   *
   * @param sql the statements to parse
   * @return the list of prepared statements.
   */
  public List<PreparedStatement<?>> parseStatements(final String sql) {
    try {
      final MetaStore parserMetaStore = metaStore.clone();

      final KsqlParser ksqlParser = new KsqlParser();

      return ksqlParser.buildAst(
          sql,
          parserMetaStore,
          stmt -> {
            validateSingleQueryAstAndUpdateParserMetaStore(stmt, parserMetaStore);
          });
    } catch (final KsqlException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(
          "Exception while processing statements: " + e.getMessage(), sql, e);
    }
  }

  /**
   * Try to execute the supplied statements. No internal state will be changed.
   *
   * <p>Statements must be executable. See {@link #isExecutableStatement(PreparedStatement)}.
   *
   * <p>No changes will be made to the meta store and no queries will be registered.
   *
   * <p>If the statements contains queries, they will not be added to the active set of queries and
   * they will have been closed before returning.
   *
   * @param statements the list of statements to execute
   * @param ksqlConfig the base ksqlConfig
   * @param overriddenProperties the property overrides
   * @return List of query metadata.
   */
  public List<QueryMetadata> tryExecute(
      final List<? extends PreparedStatement<?>> statements,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    final List<PreparedStatement<?>> toExecute = statements.stream()
        .map(statement -> {
              if (statement.getStatement() instanceof QueryContainer) {
                final Query query = ((QueryContainer) statement.getStatement()).getQuery();
                return new PreparedStatement<Statement>(statement.getStatementText(), query);
              }
              return statement;
            }
        ).collect(Collectors.toList());

    final List<QueryMetadata> queries = doExecute(
        toExecute, ksqlConfig, overriddenProperties, false);

    queries.forEach(QueryMetadata::close);

    return queries;
  }

  /**
   * Execute the supplied SQL, updating the meta store and registering the queries.
   *
   * <p>Statements must be executable. See {@link #isExecutableStatement(PreparedStatement)}.
   *
   * <p>If the SQL contains queries, they are added to the list of the engines active queries,
   * but not started.
   *
   * @param sql The SQL to execute
   * @param overriddenProperties The user-requested property overrides
   * @return List of query metadata.
   */
  public List<QueryMetadata> execute(
      final String sql,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    final List<PreparedStatement<?>> statements = parseStatements(sql);

    final List<QueryMetadata> queries = doExecute(
        statements, ksqlConfig, overriddenProperties, true);

    registerQueries(queries);

    return queries;
  }

  /**
   * Determines if a statement is executable by the engine.
   *
   * @param statement the statement to test.
   * @return {@code true} if the engine can execute the statement, {@code false} otherwise
   */
  public static boolean isExecutableStatement(final PreparedStatement<?> statement) {
    return statement.getStatement() instanceof ExecutableDdlStatement
        || statement.getStatement() instanceof QueryContainer
        || statement.getStatement() instanceof Query;
  }

  private List<QueryMetadata> doExecute(
      final List<? extends PreparedStatement<?>> statements,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final boolean updateMetastore
  ) {
    throwOnImmutableOverride(overriddenProperties);

    // Multiple queries submitted as the same time should success or fail as a whole,
    // Thus we use tempMetaStore to store newly created tables, streams or topics.
    final MetaStore tempMetaStore = metaStore.clone();

    final List<PreparedStatement<?>> postProcessed = statements.stream()
        .map(stmt -> postProcessStatement(stmt, tempMetaStore))
        .collect(Collectors.toList());

    throwOnNonExecutableStatement(postProcessed);

    final List<LogicalPlanNode> logicalPlans = queryEngine.buildLogicalPlans(
        tempMetaStore,
        postProcessed,
        ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties)
    );

    final Builder<QueryMetadata> queries = ImmutableList.builderWithExpectedSize(statements.size());
    final Map<PreparedStatement<?>, QueryMetadata> queriesByStatement =
        new IdentityHashMap<>(statements.size());

    for (int i = 0; i != logicalPlans.size(); ++i) {
      final PreparedStatement<?> statement = postProcessed.get(i);
      final LogicalPlanNode logicalPlan = logicalPlans.get(i);
      if (logicalPlan.getNode() == null) {
        if (updateMetastore) {
          doExecuteDdlStatement(
              statement.getStatementText(),
              (ExecutableDdlStatement) statement.getStatement(),
              overriddenProperties
          );
        }
      } else {
        final QueryMetadata query = queryEngine.buildPhysicalPlan(
            logicalPlan,
            ksqlConfig,
            overriddenProperties,
            clientSupplier,
            updateMetastore ? metaStore : tempMetaStore,
            updateMetastore
        );

        queries.add(query);
        queriesByStatement.put(statements.get(i), query);
      }
    }

    validateQueries(queriesByStatement);

    return queries.build();
  }

  private void validateQueries(final Map<PreparedStatement<?>, QueryMetadata> queries) {

    queries.forEach((statement, query) -> {
      if (statement.getStatement() instanceof CreateStreamAsSelect
          && query.getDataSourceType() == DataSourceType.KTABLE) {
        throw new KsqlStatementException("Invalid result type. "
            + "Your SELECT query produces a TABLE. "
            + "Please use CREATE TABLE AS SELECT statement instead.",
            statement.getStatementText());
      }

      if (statement.getStatement() instanceof CreateTableAsSelect
          && query.getDataSourceType() == DataSourceType.KSTREAM) {
        throw new KsqlStatementException("Invalid result type. "
            + "Your SELECT query produces a STREAM. "
            + "Please use CREATE STREAM AS SELECT statement instead.",
            statement.getStatementText());
      }

      if (query instanceof PersistentQueryMetadata) {
        final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
        if (!AvroUtil.isValidSchemaEvolution(persistentQuery, schemaRegistryClient)) {
          throw new KsqlStatementException(String.format(
              "Cannot register avro schema for %s as the schema registry rejected it, "
                  + "(maybe schema evolution issues?)",
              persistentQuery.getResultTopic().getKafkaTopicName()),
              statement.getStatementText());
        }
      }
    });
  }

  private void registerQueries(final List<QueryMetadata> queries) {
    for (final QueryMetadata queryMetadata : queries) {
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
    engineMetrics.registerQueries(queries);
  }

  @SuppressWarnings("unchecked")
  private void validateSingleQueryAstAndUpdateParserMetaStore(
      final PreparedStatement<?> statement,
      final MetaStore parserMetaStore
  ) {
    log.info("Building AST for {}.", statement.getStatementText());

    try {
      if (statement.getStatement() instanceof CreateAsSelect) {
        applyCreateAsSelectToMetaStore((CreateAsSelect) statement.getStatement(), parserMetaStore);
      } else if (statement.getStatement() instanceof InsertInto) {
        validateInsertIntoStatement((PreparedStatement<InsertInto>) statement, parserMetaStore);
      } else if (statement.getStatement() instanceof ExecutableDdlStatement) {
        applyDdlStatementToMetaStore(statement, parserMetaStore);
      }
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(
          "Exception while processing statement: " + e.getMessage(),
          statement.getStatementText(), e);
    }
  }

  private void applyCreateAsSelectToMetaStore(
      final CreateAsSelect statement,
      final MetaStore parserMetaStore
  ) {
    final QuerySpecification querySpecification =
        (QuerySpecification) statement.getQuery().getQueryBody();

    final StructuredDataSource resultDataSource = queryEngine.getResultDatasource(
        querySpecification.getSelect(),
        statement.getName().getSuffix()
    );

    parserMetaStore.putSource(resultDataSource.cloneWithTimeKeyColumns());
  }

  private static void validateInsertIntoStatement(
      final PreparedStatement<InsertInto> statement,
      final MetaStore parserMetaStore
  ) {
    final InsertInto insertInto = statement.getStatement();
    final String target = insertInto.getTarget().getSuffix();

    if (parserMetaStore.getSource(target) == null) {
      throw new KsqlStatementException(String.format(
          "Sink '%s' does not exist for the INSERT INTO statement.", target),
          statement.getStatementText());
    }

    if (parserMetaStore.getSource(target).getDataSourceType()
        != DataSource.DataSourceType.KSTREAM) {
      throw new KsqlStatementException(String.format(
          "INSERT INTO can only be used to insert into a stream. %s is a table.", target),
          statement.getStatementText());
    }
  }

  private void applyDdlStatementToMetaStore(
      final PreparedStatement<?> statement,
      final MetaStore parserMetaStore
  ) {
    if (statement.getStatement() instanceof SetProperty
        || statement.getStatement() instanceof UnsetProperty) {
      return;
    }

    final DdlCommand ddlCmd = createDdlCommand(
        statement.getStatementText(),
        (ExecutableDdlStatement) statement.getStatement(),
        Collections.emptyMap(),
        false);

    ddlCommandExec.tryExecute(ddlCmd, parserMetaStore);
  }

  private PreparedStatement<?> postProcessStatement(
      final PreparedStatement<?> statement,
      final MetaStore tempMetaStore
  ) {
    try {

      if (statement.getStatement() instanceof CreateAsSelect) {
        return postProcessCreateAsSelectStatement(statement);
      }

      if (statement.getStatement() instanceof InsertInto) {
        return postProcessInsertIntoStatement(statement);
      }

      if (statement.getStatement() instanceof ExecutableDdlStatement) {
        return postProcessSingleDdlStatement(statement, tempMetaStore);
      }

      return statement;
    } catch (final Exception e) {
      throw new KsqlStatementException("Exception while processing statement: " + e.getMessage(),
          statement.getStatementText(), e);
    }
  }

  private static PreparedStatement<?> postProcessCreateAsSelectStatement(
      final PreparedStatement<?> statement
  ) {
    final CreateAsSelect createAsSelect = (CreateAsSelect) statement.getStatement();

    final QuerySpecification querySpecification =
        (QuerySpecification) createAsSelect.getQuery().getQueryBody();

    final Query query = addInto(
        querySpecification,
        createAsSelect.getName().getSuffix(),
        createAsSelect.getQuery().getLimit(),
        createAsSelect.getProperties(),
        createAsSelect.getPartitionByColumn(),
        true
    );

    return new PreparedStatement<>(statement.getStatementText(), query);
  }

  private static PreparedStatement<?> postProcessInsertIntoStatement(
      final PreparedStatement<?> statement
  ) {
    final InsertInto insertInto = (InsertInto) statement.getStatement();

    final QuerySpecification querySpecification =
        (QuerySpecification) insertInto.getQuery().getQueryBody();

    final Query query = addInto(
        querySpecification,
        insertInto.getTarget().getSuffix(),
        insertInto.getQuery().getLimit(),
        new HashMap<>(),
        insertInto.getPartitionByColumn(),
        false
    );

    return new PreparedStatement<>(statement.getStatementText(), query);
  }

  private PreparedStatement<?> postProcessSingleDdlStatement(
      final PreparedStatement<?> statement,
      final MetaStore tempMetaStore) {
    if (statement.getStatement() instanceof SetProperty
        || statement.getStatement() instanceof UnsetProperty) {
      return statement;
    }

    final DdlCommand ddlCmd = createDdlCommand(
        statement.getStatementText(),
        (ExecutableDdlStatement) statement.getStatement(),
        Collections.emptyMap(),
        false);

    ddlCommandExec.tryExecute(ddlCmd, tempMetaStore);
    return statement;
  }

  private static Query addInto(
      final QuerySpecification querySpecification,
      final String intoName,
      final Optional<String> limit,
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
        querySpecification.getLocation(),
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

    return new Query(newQuerySpecification, limit);
  }

  Set<QueryMetadata> getLivePersistentQueries() {
    return livePersistentQueries;
  }

  public boolean hasActiveQueries() {
    return !livePersistentQueries.isEmpty();
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
    for (final QueryMetadata queryMetadata : allLiveQueries) {
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
      final ExecutableDdlStatement statement,
      final Map<String, Object> overriddenProperties
  ) {
    throwOnImmutableOverride(overriddenProperties);

    final DdlCommand command = createDdlCommand(
        sqlExpression,
        statement,
        overriddenProperties,
        true);

    return ddlCommandExec.execute(command, false);
  }

  public SchemaRegistryClient getSchemaRegistryClient() {
    return schemaRegistryClient;
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

  private void doExecuteDdlStatement(
      final String sqlExpression,
      final ExecutableDdlStatement statement,
      final Map<String, Object> overriddenProperties
  ) {
    final DdlCommandResult result =
        executeDdlStatement(sqlExpression, statement, overriddenProperties);

    if (!result.isSuccess()) {
      throw new KsqlStatementException(result.getMessage(), sqlExpression);
    }
  }

  private DdlCommand createDdlCommand(
      final String sqlExpression,
      final ExecutableDdlStatement statement,
      final Map<String, Object> overriddenProperties,
      final boolean enforceTopicExistence) {
    final String resultingSqlExpression;
    final ExecutableDdlStatement resultingStatement;

    if (statement instanceof AbstractStreamCreateStatement) {
      final AbstractStreamCreateStatement streamCreateStatement =
          (AbstractStreamCreateStatement) statement;

      final PreparedStatement<AbstractStreamCreateStatement> statementWithSchema
          = maybeAddFieldsFromSchemaRegistry(streamCreateStatement, sqlExpression);

      resultingStatement = (ExecutableDdlStatement) statementWithSchema.getStatement();
      resultingSqlExpression = statementWithSchema.getStatementText();

      if (((AbstractStreamCreateStatement) resultingStatement).getElements().isEmpty()) {
        throw new KsqlStatementException(
            "The statement or topic schema does not define any columns.",
            sqlExpression);
      }
    } else {
      resultingSqlExpression = sqlExpression;
      resultingStatement = statement;
    }

    return ddlCommandFactory.create(
        resultingSqlExpression, resultingStatement, overriddenProperties, enforceTopicExistence);
  }

  private PreparedStatement<AbstractStreamCreateStatement> maybeAddFieldsFromSchemaRegistry(
      final AbstractStreamCreateStatement streamCreateStatement,
      final String statementText
  ) {
    if (streamCreateStatement.getProperties().containsKey(DdlConfig.TOPIC_NAME_PROPERTY)) {
      final String ksqlRegisteredTopicName = StringUtil.cleanQuotes(
          streamCreateStatement
              .getProperties()
              .get(DdlConfig.TOPIC_NAME_PROPERTY)
              .toString()
              .toUpperCase()
      );
      final KsqlTopic ksqlTopic = metaStore.getTopic(ksqlRegisteredTopicName);
      if (ksqlTopic == null) {
        throw new KsqlStatementException(
            String.format("Could not find %s topic in the metastore.", ksqlRegisteredTopicName),
            statementText);
      }
      final Map<String, Expression> newProperties = new HashMap<>();
      newProperties.put(
          DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(ksqlTopic.getKafkaTopicName())
      );
      newProperties.put(
          DdlConfig.VALUE_FORMAT_PROPERTY,
          new StringLiteral(
              ksqlTopic.getKsqlTopicSerDe().getSerDe().toString()
          )
      );
      final AbstractStreamCreateStatement statementWithProperties = streamCreateStatement.copyWith(
          streamCreateStatement.getElements(),
          newProperties);
      return StatementWithSchema.forStatement(
          statementWithProperties,
          SqlFormatter.formatSql(statementWithProperties),
          schemaRegistryClient
      );
    }

    return StatementWithSchema.forStatement(
        streamCreateStatement,
        statementText,
        schemaRegistryClient);
  }

  private static void throwOnImmutableOverride(final Map<String, Object> overriddenProperties) {
    final String immutableProps = overriddenProperties.keySet().stream()
        .filter(IMMUTABLE_PROPERTIES::contains)
        .distinct()
        .collect(Collectors.joining(","));

    if (!immutableProps.isEmpty()) {
      throw new IllegalArgumentException("Cannot override properties: " + immutableProps);
    }
  }

  private static void throwOnNonExecutableStatement(
      final List<? extends PreparedStatement<?>> statements
  ) {
    final Predicate<PreparedStatement<?>> notExecutable = statement ->
        !isExecutableStatement(statement)
            || statement.getStatement() instanceof QueryContainer;

    final String nonExecutable = statements.stream()
        .filter(notExecutable)
        .map(PreparedStatement::getStatementText)
        .collect(Collectors.joining("\n"));

    if (!nonExecutable.isEmpty()) {
      throw new KsqlStatementException("Statement(s) not executable", nonExecutable);
    }
  }
}
