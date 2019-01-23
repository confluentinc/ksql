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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CommandFactories;
import io.confluent.ksql.ddl.commands.DdlCommand;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.ddl.commands.DdlCommandResult;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.ReadonlyMetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.metrics.StreamsErrorCollector;
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
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.planner.LogicalPlanNode;
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
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.StatementWithSchema;
import io.confluent.ksql.util.StringUtil;
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

  private final Map<QueryId, PersistentQueryMetadata> persistentQueries;
  private final Set<QueryMetadata> allLiveQueries;
  private final KsqlEngineMetrics engineMetrics;
  private final ScheduledExecutorService aggregateMetricsCollector;
  private final String serviceId;
  private final ServiceContext serviceContext;
  private final EngineContext primaryContext;

  public KsqlEngine(
      final ServiceContext serviceContext,
      final String serviceId
  ) {
    this(
        serviceContext,
        serviceId,
        new MetaStoreImpl(new InternalFunctionRegistry()),
        KsqlEngineMetrics::new);
  }

  KsqlEngine(
      final ServiceContext serviceContext,
      final String serviceId,
      final MetaStore metaStore,
      final Function<KsqlEngine, KsqlEngineMetrics> engineMetricsFactory
  ) {
    this.primaryContext = EngineContext.create(serviceContext, metaStore, this::unregisterQuery);
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
    this.persistentQueries = new HashMap<>();
    this.allLiveQueries = new HashSet<>();
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

  public long numberOfLiveQueries() {
    return allLiveQueries.size();
  }

  public long numberOfPersistentQueries() {
    return persistentQueries.size();
  }

  @Override
  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return Optional.ofNullable(persistentQueries.get(queryId));
  }

  public List<PersistentQueryMetadata> getPersistentQueries() {
    return Collections.unmodifiableList(
        new ArrayList<>(
            persistentQueries.values()));
  }

  public boolean hasActiveQueries() {
    return !persistentQueries.isEmpty();
  }

  @Override
  public MetaStore getMetaStore() {
    return ReadonlyMetaStore.readOnlyMetaStore(primaryContext.metaStore);
  }

  public FunctionRegistry getFunctionRegistry() {
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

  /**
   * Create an execution context in which statements can be run without affected the state
   * of the system.
   *
   * @return a sand boxed execution context.
   */
  @Override
  public KsqlExecutionContext createSandbox() {
    return SandboxedExecutionContext.create(primaryContext, persistentQueries);
  }

  /**
   * Parse the statement(s) in supplied {@code sql}.
   *
   * <p>Note: the engine's metas-store will not be changed.
   *
   * @param sql the statements to parse.
   * @return the list of prepared statements.
   */
  public List<PreparedStatement<?>> parseStatements(final String sql) {
    final SandboxedExecutionContext sandbox = SandboxedExecutionContext
        .create(primaryContext, persistentQueries);

    return EngineParser.create(sandbox.engineContext).buildAst(sql);
  }

  /**
   * Execute the supplied statement, updating the meta store and registering any query.
   *
   * <p>The statement must be executable. See {@link #isExecutableStatement(PreparedStatement)}.
   *
   * <p>If the statement contains a query, then it will be tracked by the engine, but not started.
   *
   * @param statement The SQL to execute.
   * @param overriddenProperties The user-requested property overrides.
   * @return List of query metadata.
   */
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
    for (final QueryMetadata queryMetadata : new HashSet<>(allLiveQueries)) {
      queryMetadata.close();
    }
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

    if (query instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
      persistentQueries.put(persistentQuery.getQueryId(), persistentQuery);
      primaryContext.metaStore.updateForPersistentQuery(
          persistentQuery.getQueryId().getId(),
          persistentQuery.getSourceNames(),
          persistentQuery.getSinkNames());
    }

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

    if (query instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
      persistentQueries.remove(persistentQuery.getQueryId());
      primaryContext.metaStore.removePersistentQuery(persistentQuery.getQueryId().getId());
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

    private final MetaStore metaStore;
    private final QueryEngine queryEngine;
    private final ServiceContext serviceContext;
    private final CommandFactories ddlCommandFactory;
    private final DdlCommandExec ddlCommandExec;

    private EngineContext(
        final ServiceContext serviceContext,
        final MetaStore metaStore,
        final Consumer<QueryMetadata> onQueryCloseCallback
    ) {
      this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
      this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
      this.ddlCommandFactory = new CommandFactories(serviceContext);
      this.queryEngine = new QueryEngine(serviceContext, onQueryCloseCallback);
      this.ddlCommandExec = new DdlCommandExec(metaStore);
    }

    private static EngineContext create(
        final ServiceContext serviceContext,
        final MetaStore metaStore,
        final Consumer<QueryMetadata> onQueryCloseCallback
    ) {
      return new EngineContext(serviceContext, metaStore, onQueryCloseCallback);
    }

    private String executeDdlStatement(
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

      final DdlCommandResult result = ddlCommandExec.execute(command);

      if (!result.isSuccess()) {
        throw new KsqlStatementException(result.getMessage(), sqlExpression);
      }

      return result.getMessage();
    }

    private DdlCommand createDdlCommand(
        final String sqlExpression,
        final ExecutableDdlStatement statement,
        final Map<String, Object> overriddenProperties,
        final boolean enforceTopicExistence
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
  }

  private static final class EngineParser {

    private final EngineContext engineContext;

    private EngineParser(final EngineContext engineContext) {
      this.engineContext = Objects.requireNonNull(engineContext, "engineContext");
    }

    private static EngineParser create(final EngineContext engineContext) {
      return new EngineParser(engineContext);
    }

    private List<PreparedStatement<?>> buildAst(final String sql) {
      try {
        final KsqlParser ksqlParser = new KsqlParser();

        return ksqlParser.buildAst(
            sql,
            engineContext.metaStore,
            this::postProcessAstStatement
        );
      } catch (final KsqlException e) {
        throw e;
      } catch (final Exception e) {
        throw new KsqlStatementException(
            "Exception while processing statements: " + e.getMessage(), sql, e);
      }
    }

    @SuppressWarnings("unchecked")
    private void postProcessAstStatement(final PreparedStatement<?> statement) {
      log.info("Building AST for {}.", statement.getStatementText());

      try {
        if (statement.getStatement() instanceof CreateAsSelect) {
          applyCreateAsSelectToMetaStore((CreateAsSelect) statement.getStatement());
        } else if (statement.getStatement() instanceof InsertInto) {
          validateInsertIntoStatement((PreparedStatement<InsertInto>) statement);
        } else if (statement.getStatement() instanceof ExecutableDdlStatement) {
          postProcessDdlStatement(statement);
        }
      } catch (final KsqlStatementException e) {
        throw e;
      } catch (final Exception e) {
        throw new KsqlStatementException(
            "Exception while processing statement: " + e.getMessage(),
            statement.getStatementText(), e);
      }
    }

    private void applyCreateAsSelectToMetaStore(final CreateAsSelect statement) {
      final QuerySpecification querySpecification =
          (QuerySpecification) statement.getQuery().getQueryBody();

      final StructuredDataSource resultDataSource = engineContext.queryEngine
          .getResultDatasource(
              querySpecification.getSelect(),
              statement.getName().getSuffix()
          );

      engineContext.metaStore.putSource(resultDataSource.cloneWithTimeKeyColumns());
    }

    private void validateInsertIntoStatement(final PreparedStatement<InsertInto> statement) {
      final InsertInto insertInto = statement.getStatement();
      final String targetName = insertInto.getTarget().getSuffix();

      final StructuredDataSource target = engineContext.metaStore.getSource(targetName);
      if (target == null) {
        throw new KsqlStatementException(String.format(
            "Sink '%s' does not exist for the INSERT INTO statement.", targetName),
            statement.getStatementText());
      }

      if (target.getDataSourceType() != DataSource.DataSourceType.KSTREAM) {
        throw new KsqlStatementException(String.format(
            "INSERT INTO can only be used to insert into a stream. %s is a table.",
            target.getName()),
            statement.getStatementText());
      }
    }

    private void postProcessDdlStatement(final PreparedStatement<?> statement) {
      if (statement.getStatement() instanceof SetProperty
          || statement.getStatement() instanceof UnsetProperty) {
        return;
      }

      final DdlCommand ddlCmd = engineContext.createDdlCommand(
          statement.getStatementText(),
          (ExecutableDdlStatement) statement.getStatement(),
          Collections.emptyMap(),
          false);

      engineContext.ddlCommandExec.execute(ddlCmd);
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

    private static EngineExecutor create(
        final EngineContext engineContext,
        final KsqlConfig ksqlConfig,
        final Map<String, Object> overriddenProperties
    ) {
      return new EngineExecutor(engineContext, ksqlConfig, overriddenProperties);
    }

    private ExecuteResult execute(final PreparedStatement<?> statement) {
      final PreparedStatement<?> postProcessed = preProcessStatement(statement);

      throwOnNonExecutableStatement(postProcessed);

      final LogicalPlanNode logicalPlan = engineContext.queryEngine.buildLogicalPlan(
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

      final QueryMetadata query = engineContext.queryEngine.buildPhysicalPlan(
          logicalPlan,
          ksqlConfig,
          overriddenProperties,
          engineContext.serviceContext.getKafkaClientSupplier(),
          engineContext.metaStore
      );

      validateQuery(query, statement);

      return ExecuteResult.of(query);
    }

    private static PreparedStatement<?> preProcessStatement(final PreparedStatement<?> stmt) {
      try {

        if (stmt.getStatement() instanceof CreateAsSelect) {
          return preProcessCreateAsSelectStatement(stmt);
        }

        if (stmt.getStatement() instanceof InsertInto) {
          return postProcessInsertIntoStatement(stmt);
        }

        return stmt;
      } catch (final Exception e) {
        throw new KsqlStatementException("Exception while processing statement: " + e.getMessage(),
            stmt.getStatementText(), e);
      }
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
  }

  private static final class SandboxedExecutionContext implements KsqlExecutionContext {

    private final EngineContext engineContext;
    private final Map<QueryId, PersistentQueryMetadata> persistentQueries;

    private static SandboxedExecutionContext create(
        final EngineContext engineContext,
        final Map<QueryId, PersistentQueryMetadata> persistentQueries
    ) {
      final EngineContext sandboxed = EngineContext.create(
          SandboxedServiceContext.create(engineContext.serviceContext),
          engineContext.metaStore.copy(),
          query -> {
          } // Do nothing on query close.
      );

      return new SandboxedExecutionContext(sandboxed, persistentQueries);
    }

    private SandboxedExecutionContext(
        final EngineContext engineContext,
        final Map<QueryId, PersistentQueryMetadata> persistentQueries
    ) {
      this.engineContext = Objects.requireNonNull(engineContext, "engineContext");
      this.persistentQueries = ImmutableMap.copyOf(persistentQueries);
    }

    @Override
    public MetaStore getMetaStore() {
      return ReadonlyMetaStore.readOnlyMetaStore(engineContext.metaStore);
    }

    @Override
    public KsqlExecutionContext createSandbox() {
      return create(engineContext, persistentQueries);
    }

    @Override
    public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
      return Optional.ofNullable(persistentQueries.get(queryId));
    }

    @Override
    public ExecuteResult execute(
        final PreparedStatement<?> statement,
        final KsqlConfig ksqlConfig,
        final Map<String, Object> overriddenProperties
    ) {
      final EngineExecutor executor =
          EngineExecutor.create(engineContext, ksqlConfig, overriddenProperties);

      final ExecuteResult result = executor.execute(statement);

      result.getQuery().ifPresent(QueryMetadata::close);

      return result;
    }
  }
}