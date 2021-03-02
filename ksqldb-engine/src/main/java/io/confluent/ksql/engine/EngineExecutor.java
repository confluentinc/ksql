/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.engine;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;

import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.physical.PhysicalPlan;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullPhysicalPlan;
import io.confluent.ksql.physical.pull.PullPhysicalPlanBuilder;
import io.confluent.ksql.physical.pull.PullQueryQueuePopulator;
import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.PullPlannerOptions;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.query.PullQueryQueue;
import io.confluent.ksql.query.QueryExecutor;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PlanSummary;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Executor of {@code PreparedStatement} within a specific {@code EngineContext} and using a
 * specific set of config.
 * </p>
 * All statements are executed using a {@code ServiceContext} specified in the constructor. This
 * {@code ServiceContext} might have been initialized with limited permissions to access Kafka
 * resources. The {@code EngineContext} has an internal {@code ServiceContext} that might have more
 * or less permissions than the one specified. This approach is useful when KSQL needs to
 * impersonate the current REST user executing the statements.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
final class EngineExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final EngineContext engineContext;
  private final ServiceContext serviceContext;
  private final SessionConfig config;

  private EngineExecutor(
      final EngineContext engineContext,
      final ServiceContext serviceContext,
      final SessionConfig config
  ) {
    this.engineContext = Objects.requireNonNull(engineContext, "engineContext");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.config = Objects.requireNonNull(config, "config");

    KsqlEngineProps.throwOnImmutableOverride(config.getOverrides());
  }

  static EngineExecutor create(
      final EngineContext engineContext,
      final ServiceContext serviceContext,
      final SessionConfig config
  ) {
    return new EngineExecutor(engineContext, serviceContext, config);
  }

  ExecuteResult execute(final KsqlPlan plan) {
    final Optional<QueryPlan> queryPlan = plan.getQueryPlan();

    if (!queryPlan.isPresent()) {
      final String ddlResult = plan
          .getDdlCommand()
          .map(ddl -> executeDdl(ddl, plan.getStatementText(), false, Collections.emptySet()))
          .orElseThrow(
              () -> new IllegalStateException(
                  "DdlResult should be present if there is no physical plan."));
      return ExecuteResult.of(ddlResult);
    }

    final Optional<String> ddlResult = plan.getDdlCommand().map(ddl ->
        executeDdl(ddl, plan.getStatementText(), true, queryPlan.get().getSources()));

    // Return if the source to create already exists.
    if (ddlResult.isPresent() && ddlResult.get().contains("already exists")) {
      return ExecuteResult.of(ddlResult.get());
    }

    return ExecuteResult.of(executePersistentQuery(
        queryPlan.get(),
        plan.getStatementText(),
        plan.getDdlCommand().isPresent())
    );
  }

  /**
   * Evaluates a pull query by first analyzing it, then building the logical plan and finally
   * the physical plan. The execution is then done using the physical plan in a pipelined manner.
   * @param statement The pull query
   * @param routingOptions Configuration parameters used for HA routing
   * @param pullQueryMetrics JMX metrics
   * @return the rows that are the result of evaluating the pull query
   */
  PullQueryResult executePullQuery(
      final ConfiguredStatement<Query> statement,
      final HARouting routing,
      final RoutingOptions routingOptions,
      final PullPlannerOptions pullPlannerOptions,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final boolean startImmediately
  ) {

    if (!statement.getStatement().isPullQuery()) {
      throw new IllegalArgumentException("Executor can only handle pull queries");
    }
    final SessionConfig sessionConfig = statement.getSessionConfig();

    try {
      final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(engineContext.getMetaStore(), "");
      final ImmutableAnalysis analysis = new RewrittenAnalysis(
          queryAnalyzer.analyze(statement.getStatement(), Optional.empty()),
          new PullQueryExecutionUtil.ColumnReferenceRewriter()::process
      );
      final KsqlConfig ksqlConfig = sessionConfig.getConfig(true);
      final LogicalPlanNode logicalPlan = buildAndValidateLogicalPlan(
          statement, analysis, ksqlConfig, pullPlannerOptions);
      final PullPhysicalPlan physicalPlan = buildPullPhysicalPlan(
          logicalPlan,
          analysis
      );
      final PullQueryQueue pullQueryQueue = new PullQueryQueue();
      final PullQueryQueuePopulator populator = () -> routing.handlePullQuery(
          serviceContext,
          physicalPlan, statement, routingOptions, physicalPlan.getOutputSchema(),
          physicalPlan.getQueryId(), pullQueryQueue);
      final PullQueryResult result = new PullQueryResult(physicalPlan.getOutputSchema(), populator,
          physicalPlan.getQueryId(), pullQueryQueue, pullQueryMetrics);
      if (startImmediately) {
        result.start();
      }
      return result;
    } catch (final Exception e) {
      pullQueryMetrics.ifPresent(metrics -> metrics.recordErrorRate(1));
      throw new KsqlStatementException(
          e.getMessage() == null
              ? "Server Error" + Arrays.toString(e.getStackTrace())
              : e.getMessage(),
          statement.getStatementText(),
          e
      );
    }
  }


  @SuppressWarnings("OptionalGetWithoutIsPresent") // Known to be non-empty
  TransientQueryMetadata executeQuery(
      final ConfiguredStatement<Query> statement,
      final boolean excludeTombstones
  ) {
    final ExecutorPlans plans = planQuery(statement, statement.getStatement(),
        Optional.empty(), Optional.empty());
    final KsqlBareOutputNode outputNode = (KsqlBareOutputNode) plans.logicalPlan.getNode().get();
    final QueryExecutor executor = engineContext.createQueryExecutor(config, serviceContext);

    engineContext.createQueryValidator().validateQuery(
        config,
        plans.physicalPlan,
        engineContext.getAllLiveQueries()
    );

    return executor.buildTransientQuery(
        statement.getStatementText(),
        plans.physicalPlan.getQueryId(),
        getSourceNames(outputNode),
        plans.physicalPlan.getPhysicalPlan(),
        buildPlanSummary(
            plans.physicalPlan.getQueryId(),
            plans.physicalPlan.getPhysicalPlan()),
        outputNode.getSchema(),
        outputNode.getLimit(),
        outputNode.getWindowInfo(),
        excludeTombstones
    );
  }

  // Known to be non-empty
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  KsqlPlan plan(final ConfiguredStatement<?> statement) {
    try {
      throwOnNonExecutableStatement(statement);

      if (statement.getStatement() instanceof ExecutableDdlStatement) {
        final DdlCommand ddlCommand = engineContext.createDdlCommand(
            statement.getStatementText(),
            (ExecutableDdlStatement) statement.getStatement(),
            config
        );

        return KsqlPlan.ddlPlanCurrent(statement.getStatementText(), ddlCommand);
      }

      final QueryContainer queryContainer = (QueryContainer) statement.getStatement();
      final ExecutorPlans plans = planQuery(
          statement,
          queryContainer.getQuery(),
          Optional.of(queryContainer.getSink()),
          queryContainer.getQueryId()
      );

      final KsqlStructuredDataOutputNode outputNode =
          (KsqlStructuredDataOutputNode) plans.logicalPlan.getNode().get();

      final Optional<DdlCommand> ddlCommand = maybeCreateSinkDdl(
          statement,
          outputNode
      );

      validateQuery(outputNode.getNodeOutputType(), statement);

      final QueryPlan queryPlan = new QueryPlan(
          getSourceNames(outputNode),
          outputNode.getIntoSourceName(),
          plans.physicalPlan.getPhysicalPlan(),
          plans.physicalPlan.getQueryId()
      );

      engineContext.createQueryValidator().validateQuery(
          config,
          plans.physicalPlan,
          engineContext.getAllLiveQueries()
      );

      return KsqlPlan.queryPlanCurrent(
          statement.getStatementText(),
          ddlCommand,
          queryPlan
      );
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(e.getMessage(), statement.getStatementText(), e);
    }
  }

  private ExecutorPlans planQuery(
      final ConfiguredStatement<?> statement,
      final Query query,
      final Optional<Sink> sink,
      final Optional<String> withQueryId) {
    final QueryEngine queryEngine = engineContext.createQueryEngine(serviceContext);
    final KsqlConfig ksqlConfig = config.getConfig(true);
    final OutputNode outputNode = QueryEngine.buildQueryLogicalPlan(
        query,
        sink,
        engineContext.getMetaStore(),
        ksqlConfig
    );
    final LogicalPlanNode logicalPlan = new LogicalPlanNode(
        statement.getStatementText(),
        Optional.of(outputNode)
    );
    final QueryId queryId = QueryIdUtil.buildId(
        engineContext,
        engineContext.idGenerator(),
        outputNode,
        ksqlConfig.getBoolean(KsqlConfig.KSQL_CREATE_OR_REPLACE_ENABLED),
        withQueryId
    );

    if (withQueryId.isPresent() && engineContext.getPersistentQuery(queryId).isPresent()) {
      throw new KsqlException(String.format("Query ID '%s' already exists.", queryId));
    }

    final PhysicalPlan physicalPlan = queryEngine.buildPhysicalPlan(
        logicalPlan,
        config,
        engineContext.getMetaStore(),
        queryId
    );
    return new ExecutorPlans(logicalPlan, physicalPlan);
  }

  private LogicalPlanNode buildAndValidateLogicalPlan(
      final ConfiguredStatement<?> statement,
      final ImmutableAnalysis analysis,
      final KsqlConfig config,
      final PullPlannerOptions pullPlannerOptions
  ) {
    final OutputNode outputNode = new LogicalPlanner(config, analysis, engineContext.getMetaStore())
        .buildPullLogicalPlan(pullPlannerOptions);
    return new LogicalPlanNode(
        statement.getStatementText(),
        Optional.of(outputNode)
    );
  }

  private PullPhysicalPlan buildPullPhysicalPlan(
      final LogicalPlanNode logicalPlan,
      final ImmutableAnalysis analysis
  ) {

    final PullPhysicalPlanBuilder builder = new PullPhysicalPlanBuilder(
        engineContext.getProcessingLogContext(),
        PullQueryExecutionUtil.findMaterializingQuery(engineContext, analysis),
        analysis
    );
    return builder.buildPullPhysicalPlan(logicalPlan);
  }

  private static final class ExecutorPlans {

    private final LogicalPlanNode logicalPlan;
    private final PhysicalPlan physicalPlan;

    private ExecutorPlans(
        final LogicalPlanNode logicalPlan,
        final PhysicalPlan physicalPlan) {
      this.logicalPlan = Objects.requireNonNull(logicalPlan, "logicalPlan");
      this.physicalPlan = Objects.requireNonNull(physicalPlan, "physicalPlanNode");
    }
  }

  private Optional<DdlCommand> maybeCreateSinkDdl(
      final ConfiguredStatement<?> cfgStatement,
      final KsqlStructuredDataOutputNode outputNode
  ) {
    if (!outputNode.createInto()) {
      validateExistingSink(outputNode);
      return Optional.empty();
    }

    final Statement statement = cfgStatement.getStatement();
    final SourceName intoSource = outputNode.getIntoSourceName();
    final boolean orReplace = statement instanceof CreateAsSelect
        && ((CreateAsSelect) statement).isOrReplace();
    final boolean ifNotExists = statement instanceof CreateAsSelect
        && ((CreateAsSelect) statement).isNotExists();

    final DataSource dataSource = engineContext.getMetaStore().getSource(intoSource);
    if (dataSource != null && !ifNotExists && !orReplace) {
      final String failedSourceType = outputNode.getNodeOutputType().getKsqlType();
      final String foundSourceType = dataSource.getDataSourceType().getKsqlType();

      throw new KsqlException(String.format(
          "Cannot add %s '%s': A %s with the same name already exists",
          failedSourceType.toLowerCase(), intoSource.text(), foundSourceType.toLowerCase()
      ));
    }

    return Optional.of(engineContext.createDdlCommand(outputNode));
  }

  private void validateExistingSink(
      final KsqlStructuredDataOutputNode outputNode
  ) {
    final SourceName name = outputNode.getIntoSourceName();
    final DataSource existing = engineContext.getMetaStore().getSource(name);

    if (existing == null) {
      throw new KsqlException(String.format("%s does not exist.", outputNode));
    }

    if (existing.getDataSourceType() != outputNode.getNodeOutputType()) {
      throw new KsqlException(String.format(
          "Incompatible data sink and query result. Data sink"
              + " (%s) type is %s but select query result is %s.",
          name.text(),
          existing.getDataSourceType(),
          outputNode.getNodeOutputType())
      );
    }

    final LogicalSchema resultSchema = outputNode.getSchema();
    final LogicalSchema existingSchema = existing.getSchema();

    if (!resultSchema.compatibleSchema(existingSchema)) {
      throw new KsqlException("Incompatible schema between results and sink."
                                  + System.lineSeparator()
                                  + "Result schema is " + resultSchema
                                  + System.lineSeparator()
                                  + "Sink schema is " + existingSchema
      );
    }
  }

  private static void validateQuery(
      final DataSourceType dataSourceType,
      final ConfiguredStatement<?> statement
  ) {
    if (statement.getStatement() instanceof CreateStreamAsSelect
        && dataSourceType == DataSourceType.KTABLE) {
      throw new KsqlStatementException("Invalid result type. "
                                           + "Your SELECT query produces a TABLE. "
                                           + "Please use CREATE TABLE AS SELECT statement instead.",
                                       statement.getStatementText());
    }

    if (statement.getStatement() instanceof CreateTableAsSelect
        && dataSourceType == DataSourceType.KSTREAM) {
      throw new KsqlStatementException(
          "Invalid result type. Your SELECT query produces a STREAM. "
           + "Please use CREATE STREAM AS SELECT statement instead.",
          statement.getStatementText());
    }
  }

  private static void throwOnNonExecutableStatement(final ConfiguredStatement<?> statement) {
    if (!KsqlEngine.isExecutableStatement(statement.getStatement())) {
      throw new KsqlStatementException("Statement not executable", statement.getStatementText());
    }
  }

  private static Set<SourceName> getSourceNames(final PlanNode outputNode) {
    return outputNode.getSourceNodes()
        .map(DataSourceNode::getDataSource)
        .map(DataSource::getName)
        .collect(Collectors.toSet());
  }

  private String executeDdl(
      final DdlCommand ddlCommand,
      final String statementText,
      final boolean withQuery,
      final Set<SourceName> withQuerySources
  ) {
    try {
      return engineContext.executeDdl(statementText, ddlCommand, withQuery, withQuerySources);
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(e.getMessage(), statementText, e);
    }
  }

  private PersistentQueryMetadata executePersistentQuery(
      final QueryPlan queryPlan,
      final String statementText,
      final boolean createAsQuery
  ) {
    final QueryExecutor executor = engineContext.createQueryExecutor(
        config,
        serviceContext
    );

    final PersistentQueryMetadata queryMetadata = executor.buildPersistentQuery(
        statementText,
        queryPlan.getQueryId(),
        engineContext.getMetaStore().getSource(queryPlan.getSink()),
        queryPlan.getSources(),
        queryPlan.getPhysicalPlan(),
        buildPlanSummary(queryPlan.getQueryId(), queryPlan.getPhysicalPlan())
    );

    engineContext.registerQuery(queryMetadata, createAsQuery);
    return queryMetadata;
  }

  private String buildPlanSummary(final QueryId queryId, final ExecutionStep<?> plan) {
    return new PlanSummary(queryId, config.getConfig(true), engineContext.getMetaStore())
        .summarize(plan);
  }
}
