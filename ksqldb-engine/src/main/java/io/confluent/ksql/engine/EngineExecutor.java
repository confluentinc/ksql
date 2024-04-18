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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.ExecutionPlan;
import io.confluent.ksql.execution.ExecutionPlanner;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.PlanInfoExtractor;
import io.confluent.ksql.execution.pull.HARouting;
import io.confluent.ksql.execution.pull.PullPhysicalPlan;
import io.confluent.ksql.execution.pull.PullPhysicalPlanBuilder;
import io.confluent.ksql.execution.pull.PullQueryQueuePopulator;
import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.execution.pull.StreamedRowTranslator;
import io.confluent.ksql.execution.scalablepush.PushPhysicalPlan;
import io.confluent.ksql.execution.scalablepush.PushPhysicalPlanBuilder;
import io.confluent.ksql.execution.scalablepush.PushPhysicalPlanCreator;
import io.confluent.ksql.execution.scalablepush.PushPhysicalPlanManager;
import io.confluent.ksql.execution.scalablepush.PushQueryPreparer;
import io.confluent.ksql.execution.scalablepush.PushQueryQueuePopulator;
import io.confluent.ksql.execution.scalablepush.PushRouting;
import io.confluent.ksql.execution.scalablepush.PushRoutingOptions;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.logicalplanner.LogicalPlan;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinedSource;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.physicalplanner.nodes.Node;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.QueryPlannerOptions;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.planner.plan.VerifiableNode;
import io.confluent.ksql.query.PullQueryWriteStream;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.QueryRegistry;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlConstants.RoutingNodeType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PlanSummary;
import io.confluent.ksql.util.PushOffsetRange;
import io.confluent.ksql.util.PushQueryMetadata;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.vertx.core.Context;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(EngineExecutor.class);
  private static final String NO_OUTPUT_TOPIC_PREFIX = "";

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
    return execute(plan, false);
  }

  ExecuteResult execute(final KsqlPlan plan, final boolean restoreInProgress) {
    if (!plan.getQueryPlan().isPresent()) {
      final String ddlResult = plan
          .getDdlCommand()
          .map(ddl -> executeDdl(ddl, plan.getStatementText(), false, Collections.emptySet(),
              restoreInProgress))
          .orElseThrow(
              () -> new IllegalStateException(
                  "DdlResult should be present if there is no physical plan."));
      return ExecuteResult.of(ddlResult);
    }

    final QueryPlan queryPlan = plan.getQueryPlan().get();
    final KsqlConstants.PersistentQueryType persistentQueryType =
        plan.getPersistentQueryType().get();

    // CREATE_SOURCE do not write to any topic. We check for read-only topics only for queries
    // that attempt to write to a sink (i.e. INSERT or CREATE_AS).
    if (persistentQueryType != KsqlConstants.PersistentQueryType.CREATE_SOURCE) {
      final DataSource sinkSource = engineContext.getMetaStore()
          .getSource(queryPlan.getSink().get());

      if (sinkSource != null && sinkSource.isSource()) {
        throw new KsqlException(String.format("Cannot insert into read-only %s: %s",
            sinkSource.getDataSourceType().getKsqlType().toLowerCase(),
            sinkSource.getName().text()));
      }
    }

    final Optional<String> ddlResult = plan.getDdlCommand().map(ddl ->
        executeDdl(ddl, plan.getStatementText(), true, queryPlan.getSources(),
            restoreInProgress));

    // Return if the source to create already exists.
    if (ddlResult.isPresent() && ddlResult.get().contains("already exists")) {
      return ExecuteResult.of(ddlResult.get());
    }

    // Do not execute the plan (found on new CST commands or commands read from the command topic)
    // for source tables if the feature is disabled. CST will still be read-only, but no query
    // must be executed.
    if (persistentQueryType == KsqlConstants.PersistentQueryType.CREATE_SOURCE
        && !isSourceTableMaterializationEnabled()) {
      QueryLogger.info(
          String.format(
              "Source table query won't be materialized because '%s' is disabled.",
              KsqlConfig.KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED
          ),
          plan.getStatementText()
      );
      return ExecuteResult.of(ddlResult.get());
    }

    return ExecuteResult.of(executePersistentQuery(
        queryPlan,
        plan.getStatementText(),
        persistentQueryType)
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
  PullQueryResult executeTablePullQuery(
      final ImmutableAnalysis analysis,
      final ConfiguredStatement<Query> statement,
      final HARouting routing,
      final RoutingOptions routingOptions,
      final QueryPlannerOptions queryPlannerOptions,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final boolean startImmediately,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {

    if (!statement.getStatement().isPullQuery()) {
      throw new IllegalArgumentException("Executor can only handle pull queries");
    }
    final SessionConfig sessionConfig = statement.getSessionConfig();

    // If we ever change how many hops a request can do, we'll need to update this for correct
    // metrics.
    final RoutingNodeType routingNodeType = routingOptions.getIsSkipForwardRequest()
        ? RoutingNodeType.REMOTE_NODE : RoutingNodeType.SOURCE_NODE;

    PullPhysicalPlan plan = null;

    try {
      // Do not set sessionConfig.getConfig to true! The copying is inefficient and slows down pull
      // query performance significantly.  Instead use QueryPlannerOptions which check overrides
      // deliberately.
      final KsqlConfig ksqlConfig = sessionConfig.getConfig(false);
      final LogicalPlanNode logicalPlan = buildAndValidateLogicalPlan(
          statement, analysis, ksqlConfig, queryPlannerOptions, false);

      // This is a cancel signal that is used to stop both local operations and requests
      final CompletableFuture<Void> shouldCancelRequests = new CompletableFuture<>();

      plan = buildPullPhysicalPlan(
          logicalPlan,
          analysis,
          queryPlannerOptions,
          shouldCancelRequests,
          consistencyOffsetVector
      );
      final PullPhysicalPlan physicalPlan = plan;

      final PullQueryWriteStream pullQueryQueue = new PullQueryWriteStream(
          analysis.getLimitClause(),
          new StreamedRowTranslator(physicalPlan.getOutputSchema(), consistencyOffsetVector));

      final PullQueryQueuePopulator populator = () -> routing.handlePullQuery(
          serviceContext,
          physicalPlan,
          statement,
          routingOptions,
          pullQueryQueue,
          shouldCancelRequests
      );

      final PullQueryResult result = new PullQueryResult(physicalPlan.getOutputSchema(), populator,
          physicalPlan.getQueryId(), pullQueryQueue, pullQueryMetrics, physicalPlan.getSourceType(),
          physicalPlan.getPlanType(), routingNodeType, physicalPlan::getRowsReadFromDataSource,
          shouldCancelRequests, consistencyOffsetVector);

      if (startImmediately) {
        result.start();
      }
      return result;
    } catch (final Exception e) {
      if (plan == null) {
        pullQueryMetrics.ifPresent(m -> m.recordErrorRateForNoResult(1));
      } else {
        final PullPhysicalPlan physicalPlan = plan;
        pullQueryMetrics.ifPresent(metrics -> metrics.recordErrorRate(1,
            physicalPlan.getSourceType(),
            physicalPlan.getPlanType(),
            routingNodeType
        ));
      }

      QueryLogger.error(
          "Failure to execute pull query",
          statement.getMaskedStatementText(),
          e
      );
      if (e instanceof KsqlStatementException) {
        throw new KsqlStatementException(
            e.getMessage() == null ? "Server Error" : e.getMessage(),
            ((KsqlStatementException) e).getUnloggedMessage(),
            statement.getMaskedStatementText(),
            e
        );
      } else {
        throw new KsqlStatementException(
            e.getMessage() == null
                ? "Server Error"
                : e.getMessage(),
            statement.getMaskedStatementText(),
            e
        );
      }
    }
  }

  ScalablePushQueryMetadata executeScalablePushQuery(
      final ImmutableAnalysis analysis,
      final ConfiguredStatement<Query> statement,
      final PushRouting pushRouting,
      final PushRoutingOptions pushRoutingOptions,
      final QueryPlannerOptions queryPlannerOptions,
      final Context context,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics
  ) {
    final SessionConfig sessionConfig = statement.getSessionConfig();

    // If we ever change how many hops a request can do, we'll need to update this for correct
    // metrics.
    final RoutingNodeType routingNodeType = pushRoutingOptions.getHasBeenForwarded()
        ? RoutingNodeType.REMOTE_NODE : RoutingNodeType.SOURCE_NODE;

    PushPhysicalPlan plan = null;

    try {
      final KsqlConfig ksqlConfig = sessionConfig.getConfig(false);
      final LogicalPlanNode logicalPlan = buildAndValidateLogicalPlan(
          statement, analysis, ksqlConfig, queryPlannerOptions, true);
      final PushPhysicalPlanCreator pushPhysicalPlanCreator = (offsetRange, catchupConsumerGroup) ->
          buildScalablePushPhysicalPlan(
              logicalPlan,
              analysis,
              context,
              offsetRange,
              catchupConsumerGroup
          );
      final Optional<PushOffsetRange> offsetRange = pushRoutingOptions.getContinuationToken()
          .map(PushOffsetRange::deserialize);
      final Optional<String> catchupConsumerGroup = pushRoutingOptions.getCatchupConsumerGroup();
      final PushPhysicalPlanManager physicalPlanManager = new PushPhysicalPlanManager(
          pushPhysicalPlanCreator, catchupConsumerGroup, offsetRange);
      final PushPhysicalPlan physicalPlan = physicalPlanManager.getPhysicalPlan();
      plan = physicalPlan;

      final TransientQueryQueue transientQueryQueue
          = new TransientQueryQueue(analysis.getLimitClause());
      final PushQueryMetadata.ResultType resultType =
          physicalPlan.getScalablePushRegistry().isTable()
          ? physicalPlan.getScalablePushRegistry().isWindowed() ? ResultType.WINDOWED_TABLE
              : ResultType.TABLE
          : ResultType.STREAM;

      final PushQueryQueuePopulator populator = () ->
          pushRouting.handlePushQuery(serviceContext, physicalPlanManager, statement,
              pushRoutingOptions, physicalPlan.getOutputSchema(), transientQueryQueue,
              scalablePushQueryMetrics, offsetRange);
      final PushQueryPreparer preparer = () ->
          pushRouting.preparePushQuery(physicalPlanManager, statement, pushRoutingOptions);
      final ScalablePushQueryMetadata metadata = new ScalablePushQueryMetadata(
          physicalPlan.getOutputSchema(),
          physicalPlan.getQueryId(),
          transientQueryQueue,
          scalablePushQueryMetrics,
          resultType,
          populator,
          preparer,
          physicalPlan.getSourceType(),
          routingNodeType,
          physicalPlan::getRowsReadFromDataSource
      );

      return metadata;
    } catch (final Exception e) {
      if (plan == null) {
        scalablePushQueryMetrics.ifPresent(m -> m.recordErrorRateForNoResult(1));
      } else {
        final PushPhysicalPlan pushPhysicalPlan = plan;
        scalablePushQueryMetrics.ifPresent(metrics -> metrics.recordErrorRate(1,
                pushPhysicalPlan.getSourceType(),
                routingNodeType
        ));
      }

      QueryLogger.error(
          "Failure to execute push query V2. "
              + pushRoutingOptions.debugString() + " "
              + queryPlannerOptions.debugString(),
          statement.getMaskedStatementText(),
          e
      );

      if (e instanceof KsqlStatementException) {
        throw new KsqlStatementException(
            e.getMessage() == null ? "Server Error" : e.getMessage(),
            ((KsqlStatementException) e).getUnloggedMessage(),
            statement.getMaskedStatementText(),
            e
        );
      } else {
        throw new KsqlStatementException(
            e.getMessage() == null
                ? "Server Error"
                : e.getMessage(),
            statement.getMaskedStatementText(),
            e
        );
      }
    }
  }

  TransientQueryMetadata executeTransientQuery(
      final ConfiguredStatement<Query> statement,
      final boolean excludeTombstones
  ) {
    final ExecutorPlans plans = planQuery(statement, statement.getStatement(),
        Optional.empty(), Optional.empty(), engineContext.getMetaStore());
    final KsqlBareOutputNode outputNode = (KsqlBareOutputNode) plans.outputNode;
    engineContext.createQueryValidator().validateQuery(
        config,
        plans.executionPlan,
        engineContext.getQueryRegistry().getAllLiveQueries()
    );
    return engineContext.getQueryRegistry().createTransientQuery(
        config,
        serviceContext,
        engineContext.getProcessingLogContext(),
        engineContext.getMetaStore(),
        statement.getMaskedStatementText(),
        plans.executionPlan.getQueryId(),
        getSourceNames(outputNode),
        plans.executionPlan.getPhysicalPlan(),
        buildPlanSummary(
            plans.executionPlan.getQueryId(),
            plans.executionPlan.getPhysicalPlan()),
        outputNode.getSchema(),
        outputNode.getLimit(),
        outputNode.getWindowInfo(),
        excludeTombstones
    );
  }

  TransientQueryMetadata executeStreamPullQuery(
      final ConfiguredStatement<Query> statement,
      final boolean excludeTombstones,
      final ImmutableMap<TopicPartition, Long> endOffsets
  ) {
    final ExecutorPlans plans = planQuery(statement, statement.getStatement(),
        Optional.empty(), Optional.empty(), engineContext.getMetaStore());
    final KsqlBareOutputNode outputNode = (KsqlBareOutputNode) plans.outputNode;
    engineContext.createQueryValidator().validateQuery(
        config,
        plans.executionPlan,
        engineContext.getQueryRegistry().getAllLiveQueries()
    );
    return engineContext.getQueryRegistry().createStreamPullQuery(
        config,
        serviceContext,
        engineContext.getProcessingLogContext(),
        engineContext.getMetaStore(),
        statement.getMaskedStatementText(),
        plans.executionPlan.getQueryId(),
        getSourceNames(outputNode),
        plans.executionPlan.getPhysicalPlan(),
        buildPlanSummary(
            plans.executionPlan.getQueryId(),
            plans.executionPlan.getPhysicalPlan()),
        outputNode.getSchema(),
        outputNode.getLimit(),
        outputNode.getWindowInfo(),
        excludeTombstones,
        endOffsets
    );
  }

  @SuppressFBWarnings(value = "NP_NULL_PARAM_DEREF_NONVIRTUAL")
  private KsqlPlan sourceTablePlan(
      final ConfiguredStatement<?> statement) {
    final CreateTable createTable = (CreateTable) statement.getStatement();
    final CreateTableCommand ddlCommand = (CreateTableCommand) engineContext.createDdlCommand(
        statement.getMaskedStatementText(),
        (ExecutableDdlStatement) statement.getStatement(),
        config
    );

    final Relation from = new AliasedRelation(
        new Table(createTable.getName()), createTable.getName());

    // Only VALUE or HEADER columns must be selected from the source table. When running a
    // pull query, the keys are added if selecting all columns.
    final Select select = new Select(
        createTable.getElements().stream()
            .filter(column -> !column.getConstraints().isKey()
                && !column.getConstraints().isPrimaryKey())
            .map(column -> new SingleColumn(
                new UnqualifiedColumnReferenceExp(column.getName()),
                Optional.of(column.getName())))
            .collect(Collectors.toList()));

    // Source table need to keep emitting changes so every new record is materialized for
    // pull query availability.
    final RefinementInfo refinementInfo = RefinementInfo.of(OutputRefinement.CHANGES);

    // This is a plan for a `select * from <source-table> emit changes` statement,
    // without a sink topic to write the results. The query is just made to materialize the
    // source table.
    final Query query = new Query(
        Optional.empty(),
        select,
        from,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(refinementInfo),
        false,
        OptionalInt.empty());

    // The source table does not exist in the current metastore, so a temporary metastore that
    // contains only the source table is created here. This metastore is used later to create
    // ExecutorsPlan.
    final MutableMetaStore tempMetastore = new MetaStoreImpl(new InternalFunctionRegistry());
    final Formats formats = ddlCommand.getFormats();
    tempMetastore.putSource(new KsqlTable<>(
        statement.getMaskedStatementText(),
        createTable.getName(),
        ddlCommand.getSchema(),
        Optional.empty(),
        false,
        new KsqlTopic(
            ddlCommand.getTopicName(),
            KeyFormat.of(formats.getKeyFormat(), formats.getKeyFeatures(), Optional.empty()),
            ValueFormat.of(formats.getValueFormat(), formats.getValueFeatures())
        ),
        true
    ), false);

    final ExecutorPlans plans = planQuery(
        statement,
        query,
        Optional.empty(),
        Optional.empty(),
        tempMetastore
    );

    final KsqlBareOutputNode outputNode =
        (KsqlBareOutputNode) plans.outputNode;

    final QueryPlan queryPlan = new QueryPlan(
        getSourceNames(outputNode),
        Optional.empty(),
        plans.executionPlan.getPhysicalPlan(),
        plans.executionPlan.getQueryId(),
        getApplicationId(plans.executionPlan.getQueryId(),
            getSourceTopicNames(outputNode))
    );

    engineContext.createQueryValidator().validateQuery(
        config,
        plans.executionPlan,
        engineContext.getQueryRegistry().getAllLiveQueries()
    );

    return KsqlPlan.queryPlanCurrent(
        statement.getMaskedStatementText(),
        Optional.of(ddlCommand),
        queryPlan);
  }

  private boolean isSourceTableMaterializationEnabled() {
    // Do not get overridden configs because this must be set only from the Server side
    return config.getConfig(false)
        .getBoolean(KsqlConfig.KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED);
  }

  KsqlPlan plan(final ConfiguredStatement<?> statement) {
    try {
      throwOnNonExecutableStatement(statement);

      if (statement.getStatement() instanceof ExecutableDdlStatement) {
        final boolean isSourceStream = statement.getStatement() instanceof CreateStream
            && ((CreateStream) statement.getStatement()).isSource();

        final boolean isSourceTable = statement.getStatement() instanceof CreateTable
            && ((CreateTable) statement.getStatement()).isSource();

        if ((isSourceStream || isSourceTable) && !isSourceTableMaterializationEnabled()) {
          throw new KsqlStatementException("Cannot execute command because source table "
              + "materialization is disabled.", statement.getMaskedStatementText());
        }

        if (isSourceTable) {
          return sourceTablePlan(statement);
        } else {
          final DdlCommand ddlCommand = engineContext.createDdlCommand(
              statement.getMaskedStatementText(),
              (ExecutableDdlStatement) statement.getStatement(),
              config
          );

          return KsqlPlan.ddlPlanCurrent(
              statement.getMaskedStatementText(),
              ddlCommand);
        }
      }

      final QueryContainer queryContainer = (QueryContainer) statement.getStatement();
      final ExecutorPlans plans = planQuery(
          statement,
          queryContainer.getQuery(),
          Optional.of(queryContainer.getSink()),
          queryContainer.getQueryId(),
          engineContext.getMetaStore()
      );

      final KsqlStructuredDataOutputNode outputNode =
          (KsqlStructuredDataOutputNode) plans.outputNode;

      final Optional<DdlCommand> ddlCommand = maybeCreateSinkDdl(
          statement,
          outputNode
      );

      validateResultType(outputNode.getNodeOutputType(), statement);

      final QueryPlan queryPlan = new QueryPlan(
          getSourceNames(outputNode),
          outputNode.getSinkName(),
          plans.executionPlan.getPhysicalPlan(),
          plans.executionPlan.getQueryId(),
          getApplicationId(plans.executionPlan.getQueryId(),
              getSourceTopicNames(outputNode))
      );

      engineContext.createQueryValidator().validateQuery(
          config,
          plans.executionPlan,
          engineContext.getQueryRegistry().getAllLiveQueries()
      );

      return KsqlPlan.queryPlanCurrent(
          statement.getMaskedStatementText(),
          ddlCommand,
          queryPlan
      );
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(e.getMessage(), statement.getMaskedStatementText(), e);
    }
  }

  private Optional<String> getApplicationId(final QueryId queryId,
                                            final Collection<String> sources) {
    return config.getConfig(true).getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)
        ? Optional.of(
        engineContext.getRuntimeAssignor()
            .getRuntimeAndMaybeAddRuntime(queryId, sources, config.getConfig(true))) :
        Optional.empty();
  }

  @SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
  private void throwIfUnsupported(final Query query) {
    if (query.isPullQuery()) { // should have been checked previously?
      throw new IllegalStateException();
    }
    if (query.getWhere().isPresent()) {
      throw new UnsupportedOperationException("New query planner does not support WHERE."
          + " Set " + KsqlConfig.KSQL_NEW_QUERY_PLANNER_ENABLED + "=false.");
    }
    if (query.getGroupBy().isPresent()) {
      throw new UnsupportedOperationException("New query planner does not support GROUP BY."
          + " Set " + KsqlConfig.KSQL_NEW_QUERY_PLANNER_ENABLED + "=false.");
    }
    if (query.getHaving().isPresent()) {
      throw new UnsupportedOperationException("New query planner does not support HAVING."
          + " Set " + KsqlConfig.KSQL_NEW_QUERY_PLANNER_ENABLED + "=false.");
    }
    if (query.getWindow().isPresent()) {
      throw new UnsupportedOperationException("New query planner does not support WINDOWS."
          + " Set " + KsqlConfig.KSQL_NEW_QUERY_PLANNER_ENABLED + "=false.");
    }
    if (query.getPartitionBy().isPresent()) {
      throw new UnsupportedOperationException("New query planner does not support PARTITION BY."
          + " Set " + KsqlConfig.KSQL_NEW_QUERY_PLANNER_ENABLED + "=false.");
    }
    if (query.getLimit().isPresent()) {
      throw new UnsupportedOperationException("New query planner does not support LIMIT."
          + " Set " + KsqlConfig.KSQL_NEW_QUERY_PLANNER_ENABLED + "=false.");
    }
    final Relation fromClause = query.getFrom();
    if (fromClause instanceof Join) {
      throw new UnsupportedOperationException("New query planner does not support joins."
          + " Set " + KsqlConfig.KSQL_NEW_QUERY_PLANNER_ENABLED + "=false.");
    }
    if (fromClause instanceof JoinedSource) {
      throw new IllegalStateException(); // top level node should always be Join
    }
  }

  private ExecutorPlans planQuery(
      final ConfiguredStatement<?> statement,
      final Query query,
      final Optional<Sink> sink,
      final Optional<String> withQueryId,
      final MetaStore metaStore) {
    final QueryEngine queryEngine = engineContext.createQueryEngine(serviceContext);
    final KsqlConfig ksqlConfig = config.getConfig(true);

    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_NEW_QUERY_PLANNER_ENABLED)) {
      throwIfUnsupported(query);

      final LogicalPlan logicalPlan =
          io.confluent.ksql.logicalplanner.LogicalPlanner.buildPlan(
              metaStore,
              query
          );

      final io.confluent.ksql.physicalplanner.PhysicalPlan physicalPlan =
          io.confluent.ksql.physicalplanner.PhysicalPlanner.buildPlan(
              metaStore,
              logicalPlan
          );

      // begin stub
      // stubbing output formats and schema for now
      final Node<?> root = physicalPlan.getRoot();

      final Builder schemaBuilder = LogicalSchema.builder();
      logicalPlan.getRoot().getOutputSchema().forEach(
          column -> {
            if (root.keyColumnNames().contains(column.name())) {
              schemaBuilder.keyColumn(column.name(), column.type());
            } else {
              schemaBuilder.valueColumn(column.name(), column.type());
            }
          }
      );
      // end stub

      return new ExecutorPlans(
          new StubbedOutputNode(
              metaStore.getSource(logicalPlan.getSourceNames().stream().findFirst().get()),
              getSinkTopic(root.getFormats(), sink.get()),
              schemaBuilder.build()
          ),
          ExecutionPlanner.buildPlan(metaStore, physicalPlan, sink.get())
      );
    } else {
      final OutputNode outputNode = QueryEngine.buildQueryLogicalPlan(
          query,
          sink,
          metaStore,
          ksqlConfig,
          statement.getMaskedStatementText()
      );

      final LogicalPlanNode logicalPlan = new LogicalPlanNode(Optional.of(outputNode));

      final QueryId queryId = QueryIdUtil.buildId(
          statement.getStatement(),
          engineContext,
          engineContext.idGenerator(),
          outputNode,
          ksqlConfig.getBoolean(KsqlConfig.KSQL_CREATE_OR_REPLACE_ENABLED),
          withQueryId
      );

      if (withQueryId.isPresent()
          && engineContext.getQueryRegistry().getPersistentQuery(queryId).isPresent()) {
        throw new KsqlException(String.format("Query ID '%s' already exists.", queryId));
      }
      final Optional<PersistentQueryMetadata> persistentQueryMetadata =
          engineContext.getQueryRegistry().getPersistentQuery(queryId);

      final Optional<PlanInfo> oldPlanInfo;

      if (persistentQueryMetadata.isPresent()) {
        final ExecutionStep<?> oldPlan = persistentQueryMetadata.get().getPhysicalPlan();
        oldPlanInfo = Optional.of(oldPlan.extractPlanInfo(new PlanInfoExtractor()));
      } else {
        oldPlanInfo = Optional.empty();
      }

      final ExecutionPlan executionPlan = queryEngine.buildPhysicalPlan(
          logicalPlan,
          config,
          metaStore,
          queryId,
          oldPlanInfo
      );

      return new ExecutorPlans(logicalPlan.getNode().get(), executionPlan);
    }
  }

  private static KsqlTopic getSinkTopic(
      final Formats formats,
      final Sink sink
  ) {
    final FormatInfo keyFormatInfo;
    final FormatInfo valueFormatInfo;
    final SerdeFeatures keyFeatures;
    final SerdeFeatures valueFeatures;

    final CreateSourceAsProperties properties = sink.getProperties();
    final Optional<String> keyFormatName = properties.getKeyFormat();
    if (keyFormatName.isPresent()) {
      keyFormatInfo = FormatInfo.of(keyFormatName.get());
      keyFeatures = SerdeFeatures.of(); // to-do
    } else {
      keyFormatInfo = formats.getKeyFormat();
      keyFeatures = formats.getKeyFeatures();
    }

    final Optional<String> valueFormatName = properties.getValueFormat();
    if (valueFormatName.isPresent()) {
      valueFormatInfo = FormatInfo.of(valueFormatName.get());
      valueFeatures = SerdeFeatures.of(); // to-do
    } else {
      valueFormatInfo = formats.getValueFormat();
      valueFeatures = formats.getValueFeatures();
    }

    final KeyFormat keyFormat = KeyFormat.nonWindowed(
        keyFormatInfo,
        keyFeatures
    );
    final ValueFormat valueFormat = ValueFormat.of(
        valueFormatInfo,
        valueFeatures
    );

    return new KsqlTopic(
        sink.getName().text(),
        keyFormat,
        valueFormat
    );
  }

  private static final class StubbedOutputNode extends KsqlStructuredDataOutputNode {
    private StubbedOutputNode(
        final DataSource source,
        final KsqlTopic sinkTopic,
        final LogicalSchema sinkSchema) {
      super(
          new PlanNodeId("stubbedOutput"),
          new StubbedVerifiableDataSourceNode(
              new PlanNodeId("stubbedSource"),
              source,
              source.getName(),
              false
          ),
          sinkSchema,
          Optional.empty(),
          sinkTopic,
          OptionalInt.empty(),
          true,
          SourceName.of(sinkTopic.getKafkaTopicName()),
          false
      );
    }

  }

  private static final class StubbedVerifiableDataSourceNode
      extends DataSourceNode
      implements VerifiableNode {

    private StubbedVerifiableDataSourceNode(
        final PlanNodeId id,
        final DataSource dataSource,
        final SourceName alias,
        final boolean isWindowed
    ) {
      super(id, dataSource, alias, isWindowed);
    }

    @Override
    public void validateKeyPresent(final SourceName sinkName) {
      // skip validation
    }

    @Override
    public LogicalSchema getSchema() {
      // return empty stub schema
      return LogicalSchema.builder().build();
    }
  }

  private LogicalPlanNode buildAndValidateLogicalPlan(
      final ConfiguredStatement<?> statement,
      final ImmutableAnalysis analysis,
      final KsqlConfig config,
      final QueryPlannerOptions queryPlannerOptions,
      final boolean isScalablePush
  ) {
    final OutputNode outputNode = new LogicalPlanner(config, analysis, engineContext.getMetaStore())
        .buildQueryLogicalPlan(queryPlannerOptions, isScalablePush);
    return new LogicalPlanNode(Optional.of(outputNode));
  }

  private PushPhysicalPlan buildScalablePushPhysicalPlan(
      final LogicalPlanNode logicalPlan,
      final ImmutableAnalysis analysis,
      final Context context,
      final Optional<PushOffsetRange> offsetRange,
      final Optional<String> catchupConsumerGroup
  ) {

    final PushPhysicalPlanBuilder builder = new PushPhysicalPlanBuilder(
        engineContext.getProcessingLogContext(),
        ScalablePushQueryExecutionUtil.findQuery(engineContext, analysis)
    );
    return builder.buildPushPhysicalPlan(logicalPlan, context, offsetRange, catchupConsumerGroup);
  }

  private PullPhysicalPlan buildPullPhysicalPlan(
      final LogicalPlanNode logicalPlan,
      final ImmutableAnalysis analysis,
      final QueryPlannerOptions queryPlannerOptions,
      final CompletableFuture<Void> shouldCancelRequests,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {

    final PullPhysicalPlanBuilder builder = new PullPhysicalPlanBuilder(
        engineContext.getProcessingLogContext(),
        PullQueryExecutionUtil.findMaterializingQuery(engineContext, analysis),
        analysis,
        queryPlannerOptions,
        shouldCancelRequests,
        consistencyOffsetVector
    );
    return builder.buildPullPhysicalPlan(logicalPlan);
  }

  private static final class ExecutorPlans {

    private final OutputNode outputNode;
    private final ExecutionPlan executionPlan;

    private ExecutorPlans(
        final OutputNode outputNode,
        final ExecutionPlan executionPlan) {
      this.outputNode = Objects.requireNonNull(outputNode, "outputNode");
      this.executionPlan = Objects.requireNonNull(executionPlan, "physicalPlanNode");
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
    final SourceName intoSource = outputNode.getSinkName().get();
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

    return Optional.of(engineContext.createDdlCommand(
        outputNode,
        ((QueryContainer) statement).getQuery().getRefinement())
    );
  }

  private void validateExistingSink(
      final KsqlStructuredDataOutputNode outputNode
  ) {
    final SourceName name = outputNode.getSinkName().get();
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

  private static void validateResultType(
      final DataSourceType dataSourceType,
      final ConfiguredStatement<?> statement
  ) {
    if (statement.getStatement() instanceof CreateStreamAsSelect
        && dataSourceType == DataSourceType.KTABLE) {
      throw new KsqlStatementException("Invalid result type. "
                                           + "Your SELECT query produces a TABLE. "
                                           + "Please use CREATE TABLE AS SELECT statement instead.",
                                       statement.getMaskedStatementText());
    }

    if (statement.getStatement() instanceof CreateTableAsSelect
        && dataSourceType == DataSourceType.KSTREAM) {
      throw new KsqlStatementException(
          "Invalid result type. Your SELECT query produces a STREAM. "
           + "Please use CREATE STREAM AS SELECT statement instead.",
          statement.getMaskedStatementText());
    }
  }

  private static void throwOnNonExecutableStatement(final ConfiguredStatement<?> statement) {
    if (!KsqlEngine.isExecutableStatement(statement.getStatement())) {
      throw new KsqlStatementException("Statement not executable",
          statement.getMaskedStatementText());
    }
  }

  private static Set<SourceName> getSourceNames(final PlanNode outputNode) {
    return outputNode.getSourceNodes()
        .map(DataSourceNode::getDataSource)
        .map(DataSource::getName)
        .collect(Collectors.toSet());
  }

  private static Set<String> getSourceTopicNames(final PlanNode outputNode) {
    return outputNode.getSourceNodes()
        .map(DataSourceNode::getDataSource)
        .map(DataSource::getKsqlTopic)
        .map(KsqlTopic::getKafkaTopicName)
        .collect(Collectors.toSet());
  }

  private String executeDdl(
      final DdlCommand ddlCommand,
      final String statementText,
      final boolean withQuery,
      final Set<SourceName> withQuerySources,
      final boolean restoreInProgress
  ) {
    try {
      return engineContext.executeDdl(statementText, ddlCommand, withQuery, withQuerySources,
          restoreInProgress);
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(e.getMessage(), statementText, e);
    }
  }

  private Set<DataSource> getSources(final QueryPlan queryPlan) {
    final ImmutableSet.Builder<DataSource> sources = ImmutableSet.builder();
    for (final SourceName name : queryPlan.getSources()) {
      final DataSource dataSource = engineContext.getMetaStore().getSource(name);
      if (dataSource == null) {
        throw new KsqlException("Unknown source: " + name.toString(FormatOptions.noEscape()));
      }

      sources.add(dataSource);
    }

    return sources.build();
  }

  private PersistentQueryMetadata executePersistentQuery(
      final QueryPlan queryPlan,
      final String statementText,
      final KsqlConstants.PersistentQueryType persistentQueryType
  ) {
    final QueryRegistry queryRegistry = engineContext.getQueryRegistry();
    return queryRegistry.createOrReplacePersistentQuery(
        config,
        serviceContext,
        engineContext.getProcessingLogContext(),
        engineContext.getMetaStore(),
        statementText,
        queryPlan.getQueryId(),
        queryPlan.getSink().map(s -> engineContext.getMetaStore().getSource(s)),
        getSources(queryPlan),
        queryPlan.getPhysicalPlan(),
        buildPlanSummary(queryPlan.getQueryId(), queryPlan.getPhysicalPlan()),
        persistentQueryType,
        queryPlan.getRuntimeId()
    );
  }

  private String buildPlanSummary(final QueryId queryId, final ExecutionStep<?> plan) {
    return new PlanSummary(queryId, config.getConfig(true), engineContext.getMetaStore())
        .summarize(plan);
  }
}
