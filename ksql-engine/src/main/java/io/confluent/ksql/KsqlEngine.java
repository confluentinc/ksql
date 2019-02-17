/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CommandFactories;
import io.confluent.ksql.ddl.commands.DdlCommand;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.ddl.commands.DdlCommandResult;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.metrics.StreamsErrorCollector;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
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
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.processing.log.ProcessingLogContext;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.AvroUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.StatementWithSchema;
import io.confluent.ksql.util.StringUtil;
import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlEngine implements KsqlExecutionContext, Closeable {

  private static final Logger log = LoggerFactory.getLogger(KsqlEngine.class);

  private static final Set<String> IMMUTABLE_PROPERTIES = ImmutableSet.<String>builder()
      .add(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)
      .add(KsqlConfig.KSQL_EXT_DIR)
      .add(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG)
      .addAll(KsqlConfig.SSL_CONFIG_NAMES)
      .build();

  private final AtomicBoolean acceptingStatements = new AtomicBoolean(true);
  private final Set<QueryMetadata> allLiveQueries = ConcurrentHashMap.newKeySet();
  private final KsqlEngineMetrics engineMetrics;
  private final ScheduledExecutorService aggregateMetricsCollector;
  private final String serviceId;
  private final ServiceContext serviceContext;
  private final EngineContext primaryContext;

  public KsqlEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final String serviceId
  ) {
    this(
        serviceContext,
        processingLogContext,
        serviceId,
        new MetaStoreImpl(functionRegistry),
        KsqlEngineMetrics::new);
  }

  KsqlEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final String serviceId,
      final MutableMetaStore metaStore,
      final Function<KsqlEngine, KsqlEngineMetrics> engineMetricsFactory
  ) {
    this.primaryContext = EngineContext.create(
        serviceContext,
        processingLogContext,
        metaStore,
        new QueryIdGenerator(),
        this::unregisterQuery);
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
    this.engineMetrics = engineMetricsFactory.apply(this);
    this.aggregateMetricsCollector = Executors.newSingleThreadScheduledExecutor();
    this.aggregateMetricsCollector.scheduleAtFixedRate(
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

  public int numberOfLiveQueries() {
    return allLiveQueries.size();
  }

  @Override
  public int numberOfPersistentQueries() {
    return primaryContext.numberOfPersistentQueries();
  }

  @Override
  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return primaryContext.getPersistentQuery(queryId);
  }

  public List<PersistentQueryMetadata> getPersistentQueries() {
    return ImmutableList.copyOf(primaryContext.persistentQueries.values());
  }

  public boolean hasActiveQueries() {
    return !primaryContext.persistentQueries.isEmpty();
  }

  @Override
  public MetaStore getMetaStore() {
    return primaryContext.metaStore;
  }

  public DdlCommandExec getDdlCommandExec() {
    return primaryContext.ddlCommandExec;
  }

  public String getServiceId() {
    return serviceId;
  }

  public static Set<String> getImmutableProperties() {
    return IMMUTABLE_PROPERTIES;
  }

  public void stopAcceptingStatements() {
    acceptingStatements.set(false);
  }

  public boolean isAcceptingStatements() {
    return acceptingStatements.get();
  }

  @Override
  public KsqlExecutionContext createSandbox() {
    return new SandboxedExecutionContext(primaryContext);
  }

  @Override
  public List<ParsedStatement> parse(final String sql) {
    return primaryContext.parse(sql);
  }

  @Override
  public PreparedStatement<?> prepare(final ParsedStatement stmt) {
    return primaryContext.prepare(stmt);
  }

  @Override
  public ExecuteResult execute(
      final PreparedStatement<?> statement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    final ExecuteResult result =
        EngineExecutor.create(primaryContext, ksqlConfig, overriddenProperties)
            .execute(statement);

    result.getQuery().ifPresent(this::registerQuery);

    return result;
  }

  @Override
  public void close() {
    allLiveQueries.forEach(QueryMetadata::close);
    engineMetrics.close();
    aggregateMetricsCollector.shutdown();
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

  private void registerQuery(final QueryMetadata query) {
    allLiveQueries.add(query);
    engineMetrics.registerQuery(query);
  }

  private void unregisterQuery(final QueryMetadata query) {
    final String applicationId = query.getQueryApplicationId();

    if (!query.getState().equalsIgnoreCase("NOT_RUNNING")) {
      throw new IllegalStateException("query not stopped."
          + " id " + applicationId + ", state: " + query.getState());
    }

    if (!allLiveQueries.remove(query)) {
      return;
    }

    if (query.hasEverBeenStarted()) {
      SchemaRegistryUtil
          .cleanUpInternalTopicAvroSchemas(applicationId, serviceContext.getSchemaRegistryClient());
      serviceContext.getTopicClient().deleteInternalTopics(applicationId);
    }

    StreamsErrorCollector.notifyApplicationClose(applicationId);
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

  private static void throwOnNonExecutableStatement(final PreparedStatement<?> statement) {
    if (!isExecutableStatement(statement)) {
      throw new KsqlStatementException("Statement not executable", statement.getStatementText());
    }
  }

  private static final class EngineContext {

    private final MutableMetaStore metaStore;
    private final ServiceContext serviceContext;
    private final CommandFactories ddlCommandFactory;
    private final DdlCommandExec ddlCommandExec;
    private final QueryIdGenerator queryIdGenerator;
    private final ProcessingLogContext processingLogContext;
    private final KsqlParser parser = new DefaultKsqlParser();
    private final Consumer<QueryMetadata> outerOnQueryCloseCallback;
    private final Map<QueryId, PersistentQueryMetadata> persistentQueries;

    private EngineContext(
        final ServiceContext serviceContext,
        final ProcessingLogContext processingLogContext,
        final MutableMetaStore metaStore,
        final QueryIdGenerator queryIdGenerator,
        final Consumer<QueryMetadata> onQueryCloseCallback
    ) {
      this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
      this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
      this.queryIdGenerator = Objects.requireNonNull(queryIdGenerator, "queryIdGenerator");
      this.ddlCommandFactory = new CommandFactories(serviceContext);
      this.outerOnQueryCloseCallback = Objects
          .requireNonNull(onQueryCloseCallback, "onQueryCloseCallback");
      this.ddlCommandExec = new DdlCommandExec(metaStore);
      this.persistentQueries = new ConcurrentHashMap<>();
      this.processingLogContext = Objects
          .requireNonNull(processingLogContext, "processingLogContext");
    }

    static EngineContext create(
        final ServiceContext serviceContext,
        final ProcessingLogContext processingLogContext,
        final MutableMetaStore metaStore,
        final QueryIdGenerator queryIdGenerator,
        final Consumer<QueryMetadata> onQueryCloseCallback
    ) {
      return new EngineContext(
          serviceContext,
          processingLogContext,
          metaStore,
          queryIdGenerator,
          onQueryCloseCallback);
    }

    QueryEngine createQueryEngine() {
      return new QueryEngine(
          serviceContext,
          processingLogContext,
          queryIdGenerator,
          this::unregisterQuery);
    }

    String executeDdlStatement(
        final String sqlExpression,
        final ExecutableDdlStatement statement,
        final Map<String, Object> overriddenProperties
    ) {
      throwOnImmutableOverride(overriddenProperties);

      final DdlCommand command = createDdlCommand(
          sqlExpression,
          statement,
          overriddenProperties
      );

      final DdlCommandResult result = ddlCommandExec.execute(command);

      if (!result.isSuccess()) {
        throw new KsqlStatementException(result.getMessage(), sqlExpression);
      }

      return result.getMessage();
    }

    private DdlCommand createDdlCommand(
        final String sqlExpression,
        final ExecutableDdlStatement statement,
        final Map<String, Object> overriddenProperties
    ) {
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
          resultingSqlExpression, resultingStatement, overriddenProperties);
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

        final AbstractStreamCreateStatement statementWithProperties =
            streamCreateStatement.copyWith(
                streamCreateStatement.getElements(),
                newProperties);

        return StatementWithSchema.forStatement(
            statementWithProperties,
            SqlFormatter.formatSql(statementWithProperties),
            serviceContext.getSchemaRegistryClient()
        );
      }

      return StatementWithSchema.forStatement(
          streamCreateStatement,
          statementText,
          serviceContext.getSchemaRegistryClient());
    }

    void registerQuery(final QueryMetadata query) {
      if (query instanceof PersistentQueryMetadata) {
        final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
        final QueryId queryId = persistentQuery.getQueryId();

        if (persistentQueries.putIfAbsent(queryId, persistentQuery) != null) {
          throw new IllegalStateException("Query already registered:" + queryId);
        }

        metaStore.updateForPersistentQuery(
            queryId.getId(),
            persistentQuery.getSourceNames(),
            persistentQuery.getSinkNames());
      }
    }

    private void unregisterQuery(final QueryMetadata query) {
      if (query instanceof PersistentQueryMetadata) {
        final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
        persistentQueries.remove(persistentQuery.getQueryId());
        metaStore.removePersistentQuery(persistentQuery.getQueryId().getId());
      }

      outerOnQueryCloseCallback.accept(query);
    }

    int numberOfPersistentQueries() {
      return persistentQueries.size();
    }

    Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
      return Optional.ofNullable(persistentQueries.get(queryId));
    }

    List<ParsedStatement> parse(final String sql) {
      return parser.parse(sql);
    }

    PreparedStatement<?> prepare(final ParsedStatement stmt) {
      try {
        return parser.prepare(stmt, metaStore);
      } catch (final KsqlException e) {
        throw e;
      } catch (final Exception e) {
        throw new KsqlStatementException(
            "Exception while preparing statement: " + e.getMessage(), stmt.getStatementText(), e);
      }
    }
  }

  private static final class EngineExecutor {

    private final EngineContext engineContext;
    private final KsqlConfig ksqlConfig;
    private final Map<String, Object> overriddenProperties;

    private EngineExecutor(
        final EngineContext engineContext,
        final KsqlConfig ksqlConfig,
        final Map<String, Object> overriddenProperties
    ) {
      this.engineContext = Objects.requireNonNull(engineContext, "engineContext");
      this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
      this.overriddenProperties =
          Objects.requireNonNull(overriddenProperties, "overriddenProperties");

      throwOnImmutableOverride(overriddenProperties);
    }

    static EngineExecutor create(
        final EngineContext engineContext,
        final KsqlConfig ksqlConfig,
        final Map<String, Object> overriddenProperties
    ) {
      return new EngineExecutor(engineContext, ksqlConfig, overriddenProperties);
    }

    ExecuteResult execute(final PreparedStatement<?> statement) {
      try {
        final PreparedStatement<?> postProcessed = preProcessStatement(statement);

        throwOnNonExecutableStatement(postProcessed);

        final QueryEngine queryEngine = engineContext.createQueryEngine();

        final LogicalPlanNode logicalPlan = queryEngine.buildLogicalPlan(
            engineContext.metaStore,
            postProcessed,
            ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties)
        );

        if (logicalPlan.getNode() == null) {
          final String msg = engineContext.executeDdlStatement(
              statement.getStatementText(),
              (ExecutableDdlStatement) statement.getStatement(),
              overriddenProperties
          );

          return ExecuteResult.of(msg);
        }

        final QueryMetadata query = queryEngine.buildPhysicalPlan(
            logicalPlan,
            ksqlConfig,
            overriddenProperties,
            engineContext.serviceContext.getKafkaClientSupplier(),
            engineContext.metaStore
        );

        validateQuery(query, statement);

        engineContext.registerQuery(query);

        return ExecuteResult.of(query);
      } catch (final KsqlStatementException e) {
        throw e;
      } catch (final Exception e) {
        throw new KsqlStatementException(e.getMessage(), statement.getStatementText(), e);
      }
    }

    private PreparedStatement<?> preProcessStatement(final PreparedStatement<?> stmt) {
      if (stmt.getStatement() instanceof CreateAsSelect) {
        return preProcessCreateAsSelectStatement(stmt);
      }

      if (stmt.getStatement() instanceof InsertInto) {
        return preProcessInsertIntoStatement(stmt);
      }

      return stmt;
    }

    private static PreparedStatement<?> preProcessCreateAsSelectStatement(
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

      return PreparedStatement.of(statement.getStatementText(), query);
    }

    private PreparedStatement<?> preProcessInsertIntoStatement(
        final PreparedStatement<?> statement
    ) {
      final InsertInto insertInto = (InsertInto) statement.getStatement();

      final String targetName = insertInto.getTarget().getSuffix();
      final StructuredDataSource target = engineContext.metaStore.getSource(targetName);
      if (target == null) {
        throw new KsqlStatementException(
            "Sink does not exist for the INSERT INTO statement: " + targetName,
            statement.getStatementText());
      }

      if (target.getDataSourceType() != DataSource.DataSourceType.KSTREAM) {
        throw new KsqlStatementException(String.format(
            "INSERT INTO can only be used to insert into a stream. %s is a table.",
            target.getName()),
            statement.getStatementText());
      }

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

      return PreparedStatement.of(statement.getStatementText(), query);
    }

    private void validateQuery(final QueryMetadata query, final PreparedStatement<?> statement) {
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
        final SchemaRegistryClient srClient = engineContext.serviceContext
            .getSchemaRegistryClient();

        if (!AvroUtil.isValidSchemaEvolution(persistentQuery, srClient)) {
          throw new KsqlStatementException(String.format(
              "Cannot register avro schema for %s as the schema registry rejected it, "
                  + "(maybe schema evolution issues?)",
              persistentQuery.getResultTopic().getKafkaTopicName()),
              statement.getStatementText());
        }
      }
    }

    private static Query addInto(
        final QuerySpecification querySpecification,
        final String intoName,
        final OptionalInt limit,
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
  }

  private static final class SandboxedExecutionContext implements KsqlExecutionContext {

    private final EngineContext engineContext;

    SandboxedExecutionContext(final EngineContext sourceContext) {
      this.engineContext = EngineContext.create(
          SandboxedServiceContext.create(sourceContext.serviceContext),
          sourceContext.processingLogContext,
          sourceContext.metaStore.copy(),
          sourceContext.queryIdGenerator.copy(),
          query -> {
            // No-op
          }
      );

      sourceContext.persistentQueries.forEach((queryId, query) ->
          engineContext.persistentQueries.put(
              query.getQueryId(),
              query.copyWith(engineContext::unregisterQuery)));
    }

    @Override
    public MetaStore getMetaStore() {
      return engineContext.metaStore;
    }

    @Override
    public KsqlExecutionContext createSandbox() {
      return new SandboxedExecutionContext(engineContext);
    }

    @Override
    public int numberOfPersistentQueries() {
      return engineContext.numberOfPersistentQueries();
    }

    @Override
    public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
      return engineContext.getPersistentQuery(queryId);
    }

    @Override
    public List<ParsedStatement> parse(final String sql) {
      return engineContext.parse(sql);
    }

    @Override
    public PreparedStatement<?> prepare(final ParsedStatement stmt) {
      return engineContext.prepare(stmt);
    }

    @Override
    public ExecuteResult execute(
        final PreparedStatement<?> statement,
        final KsqlConfig ksqlConfig,
        final Map<String, Object> overriddenProperties
    ) {
      final EngineExecutor executor =
          EngineExecutor.create(engineContext, ksqlConfig, overriddenProperties);

      return executor.execute(statement);
    }
  }
}