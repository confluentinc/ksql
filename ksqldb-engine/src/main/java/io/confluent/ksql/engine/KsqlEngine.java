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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metrics.StreamsErrorCollector;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.physical.scalablepush.PushRouting;
import io.confluent.ksql.physical.scalablepush.PushRoutingOptions;
import io.confluent.ksql.planner.QueryPlannerOptions;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.vertx.core.Context;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlEngine implements KsqlExecutionContext, Closeable {

  private static final Logger log = LoggerFactory.getLogger(KsqlEngine.class);

  private final KsqlEngineMetrics engineMetrics;
  private final ScheduledExecutorService aggregateMetricsCollector;
  private final String serviceId;
  private final EngineContext primaryContext;
  private final QueryCleanupService cleanupService;
  private final OrphanedTransientQueryCleaner orphanedTransientQueryCleaner;

  public KsqlEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final ServiceInfo serviceInfo,
      final QueryIdGenerator queryIdGenerator,
      final KsqlConfig ksqlConfig,
      final List<QueryEventListener> queryEventListeners
  ) {
    this(
        serviceContext,
        processingLogContext,
        serviceInfo.serviceId(),
        new MetaStoreImpl(functionRegistry),
        (engine) -> new KsqlEngineMetrics(
            serviceInfo.metricsPrefix(),
            engine,
            serviceInfo.customMetricsTags(),
            serviceInfo.metricsExtension()
        ),
        queryIdGenerator,
        ksqlConfig,
        queryEventListeners
    );
  }

  public KsqlEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final String serviceId,
      final MutableMetaStore metaStore,
      final Function<KsqlEngine, KsqlEngineMetrics> engineMetricsFactory,
      final QueryIdGenerator queryIdGenerator,
      final KsqlConfig ksqlConfig,
      final List<QueryEventListener> queryEventListeners
  ) {
    this.cleanupService = new QueryCleanupService();
    this.orphanedTransientQueryCleaner =
        new OrphanedTransientQueryCleaner(this.cleanupService, ksqlConfig);
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
    this.engineMetrics = engineMetricsFactory.apply(this);
    this.primaryContext = EngineContext.create(
        serviceContext,
        processingLogContext,
        metaStore,
        queryIdGenerator,
        cleanupService,
        ksqlConfig,
        ImmutableList.<QueryEventListener>builder()
            .addAll(queryEventListeners)
            .add(engineMetrics.getQueryEventListener())
            .add(new CleanupListener(cleanupService, serviceContext, ksqlConfig))
            .build()
    );
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

    cleanupService.startAsync();
  }

  public int numberOfLiveQueries() {
    return primaryContext.getQueryRegistry().getAllLiveQueries().size();
  }

  @Override
  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return primaryContext.getQueryRegistry().getPersistentQuery(queryId);
  }

  @Override
  public List<PersistentQueryMetadata> getPersistentQueries() {
    return ImmutableList.copyOf(primaryContext.getQueryRegistry().getPersistentQueries().values());
  }

  @Override
  public Set<QueryId> getQueriesWithSink(final SourceName sourceName) {
    return primaryContext.getQueryRegistry().getQueriesWithSink(sourceName);
  }

  @Override
  public List<QueryMetadata> getAllLiveQueries() {
    return primaryContext.getQueryRegistry().getAllLiveQueries();
  }

  public boolean hasActiveQueries() {
    return !primaryContext.getQueryRegistry().getPersistentQueries().isEmpty();
  }

  @Override
  public MetaStore getMetaStore() {
    return primaryContext.getMetaStore();
  }

  @Override
  public ServiceContext getServiceContext() {
    return primaryContext.getServiceContext();
  }

  @Override
  public ProcessingLogContext getProcessingLogContext() {
    return primaryContext.getProcessingLogContext();
  }

  public String getServiceId() {
    return serviceId;
  }

  @VisibleForTesting
  QueryCleanupService getCleanupService() {
    return cleanupService;
  }

  @Override
  public KsqlExecutionContext createSandbox(final ServiceContext serviceContext) {
    return new SandboxedExecutionContext(primaryContext, serviceContext);
  }

  @Override
  public List<ParsedStatement> parse(final String sql) {
    return primaryContext.parse(sql);
  }

  @Override
  public PreparedStatement<?> prepare(
      final ParsedStatement stmt,
      final Map<String, String> variablesMap
  ) {
    return primaryContext.prepare(stmt, variablesMap);
  }

  @Override
  public KsqlPlan plan(
      final ServiceContext serviceContext,
      final ConfiguredStatement<?> statement
  ) {
    return EngineExecutor
        .create(primaryContext, serviceContext, statement.getSessionConfig())
        .plan(statement);
  }

  @Override
  public ExecuteResult execute(final ServiceContext serviceContext, final ConfiguredKsqlPlan plan) {
    try {
      final ExecuteResult result = EngineExecutor
          .create(primaryContext, serviceContext, plan.getConfig())
          .execute(plan.getPlan());
      return result;
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final KsqlException e) {
      // add the statement text to the KsqlException
      throw new KsqlStatementException(
          e.getMessage(),
          e.getMessage(),
          plan.getPlan().getStatementText(),
          e.getCause()
      );
    }
  }

  @Override
  public ExecuteResult execute(
      final ServiceContext serviceContext,
      final ConfiguredStatement<?> statement
  ) {
    return execute(
        serviceContext,
        ConfiguredKsqlPlan.of(
            plan(serviceContext, statement),
            statement.getSessionConfig()
        )
    );
  }

  @Override
  public TransientQueryMetadata executeQuery(
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final boolean excludeTombstones
  ) {
    try {
      final TransientQueryMetadata query = EngineExecutor
          .create(primaryContext, serviceContext, statement.getSessionConfig())
          .executeQuery(statement, excludeTombstones);
      return query;
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final KsqlException e) {
      // add the statement text to the KsqlException
      throw new KsqlStatementException(e.getMessage(), statement.getMaskedStatementText(),
          e.getCause());
    }
  }

  @Override
  public ScalablePushQueryMetadata executeScalablePushQuery(
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final PushRouting pushRouting,
      final PushRoutingOptions pushRoutingOptions,
      final QueryPlannerOptions queryPlannerOptions,
      final Context context
  ) {
    final ScalablePushQueryMetadata query = EngineExecutor
        .create(primaryContext, serviceContext, statement.getSessionConfig())
        .executeScalablePushQuery(statement, pushRouting, pushRoutingOptions, queryPlannerOptions,
            context);
    return query;
  }

  @Override
  public PullQueryResult executePullQuery(
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final HARouting routing,
      final RoutingOptions routingOptions,
      final QueryPlannerOptions plannerOptions,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final boolean startImmediately
  ) {
    return EngineExecutor
        .create(
            primaryContext,
            serviceContext,
            statement.getSessionConfig()
        )
        .executePullQuery(
            statement,
            routing,
            routingOptions,
            plannerOptions,
            pullQueryMetrics,
            startImmediately
        );
  }

  /**
   * @param closeQueries whether or not to clean up the local state for any running queries
   */
  public void close(final boolean closeQueries) {
    primaryContext.getQueryRegistry().close(closeQueries);

    try {
      cleanupService.stopAsync().awaitTerminated(30, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      log.warn("Timed out while closing cleanup service. "
              + "External resources for the following applications may be orphaned: {}",
          cleanupService.pendingApplicationIds()
      );
    }

    engineMetrics.close();
    aggregateMetricsCollector.shutdown();
  }

  @Override
  public void close() {
    close(false);
  }

  public void cleanupOrphanedInternalTopics(
      final ServiceContext serviceContext,
      final Set<String> queryApplicationIds
  ) {
    orphanedTransientQueryCleaner
        .cleanupOrphanedInternalTopics(serviceContext, queryApplicationIds);
  }

  /**
   * Determines if a statement is executable by the engine.
   *
   * @param statement the statement to test.
   * @return {@code true} if the engine can execute the statement, {@code false} otherwise
   */
  public static boolean isExecutableStatement(final Statement statement) {
    return statement instanceof ExecutableDdlStatement
        || statement instanceof QueryContainer
        || statement instanceof Query;
  }

  private static final class CleanupListener implements QueryEventListener {
    final QueryCleanupService cleanupService;
    final ServiceContext serviceContext;
    final KsqlConfig ksqlConfig;

    private CleanupListener(
        final QueryCleanupService cleanupService,
        final ServiceContext serviceContext,
        final KsqlConfig ksqlConfig) {
      this.cleanupService = cleanupService;
      this.serviceContext = serviceContext;
      this.ksqlConfig = ksqlConfig;
    }

    @Override
    public void onClose(
        final QueryMetadata query
    ) {
      final String applicationId = query.getQueryApplicationId();
      if (query.hasEverBeenStarted()) {
        cleanupService.addCleanupTask(
            new QueryCleanupService.QueryCleanupTask(
                serviceContext,
                applicationId,
                query instanceof TransientQueryMetadata,
                ksqlConfig.getKsqlStreamConfigProps()
                    .getOrDefault(
                        StreamsConfig.STATE_DIR_CONFIG,
                        StreamsConfig.configDef()
                          .defaultValues()
                          .get(StreamsConfig.STATE_DIR_CONFIG))
                    .toString()
            ));
      }

      StreamsErrorCollector.notifyApplicationClose(applicationId);
    }
  }
}
