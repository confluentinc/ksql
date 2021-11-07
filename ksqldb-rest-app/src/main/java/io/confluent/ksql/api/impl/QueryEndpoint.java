/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.server.QueryHandle;
import io.confluent.ksql.api.server.SlidingWindowRateLimiter;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.PullQueryExecutionUtil;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.physical.scalablepush.PushRouting;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.LocalCommands;
import io.confluent.ksql.rest.server.resources.streaming.PullQueryConfigPlannerOptions;
import io.confluent.ksql.rest.server.resources.streaming.PullQueryConfigRoutingOptions;
import io.confluent.ksql.rest.server.resources.streaming.PushQueryConfigPlannerOptions;
import io.confluent.ksql.rest.server.resources.streaming.PushQueryConfigRoutingOptions;
import io.confluent.ksql.rest.util.ConcurrencyLimiter;
import io.confluent.ksql.rest.util.ConcurrencyLimiter.Decrementer;
import io.confluent.ksql.rest.util.QueryCapacityUtil;
import io.confluent.ksql.rest.util.QueryMetricsUtil;
import io.confluent.ksql.rest.util.ScalablePushUtil;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PushQueryMetadata;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.StreamPullQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class QueryEndpoint {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final KsqlEngine ksqlEngine;
  private final KsqlConfig ksqlConfig;
  private final KsqlRestConfig ksqlRestConfig;
  private final RoutingFilterFactory routingFilterFactory;
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;
  private final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics;
  private final RateLimiter rateLimiter;
  private final ConcurrencyLimiter pullConcurrencyLimiter;
  private final SlidingWindowRateLimiter pullBandRateLimiter;
  private final SlidingWindowRateLimiter scalablePushBandRateLimiter;
  private final HARouting routing;
  private final PushRouting pushRouting;
  private final Optional<LocalCommands> localCommands;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public QueryEndpoint(
      // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final KsqlRestConfig ksqlRestConfig,
      final RoutingFilterFactory routingFilterFactory,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
      final RateLimiter rateLimiter,
      final ConcurrencyLimiter pullConcurrencyLimiter,
      final SlidingWindowRateLimiter pullBandLimiter,
      final SlidingWindowRateLimiter scalablePushBandRateLimiter,
      final HARouting routing,
      final PushRouting pushRouting,
      final Optional<LocalCommands> localCommands
  ) {
    this.ksqlEngine = ksqlEngine;
    this.ksqlConfig = ksqlConfig;
    this.ksqlRestConfig = ksqlRestConfig;
    this.routingFilterFactory = routingFilterFactory;
    this.pullQueryMetrics = pullQueryMetrics;
    this.scalablePushQueryMetrics = scalablePushQueryMetrics;
    this.rateLimiter = rateLimiter;
    this.pullConcurrencyLimiter = pullConcurrencyLimiter;
    this.pullBandRateLimiter = pullBandLimiter;
    this.scalablePushBandRateLimiter = scalablePushBandRateLimiter;
    this.routing = routing;
    this.pushRouting = pushRouting;
    this.localCommands = localCommands;
  }

  public QueryPublisher createQueryPublisher(
      final String sql,
      final Map<String, Object> properties,
      final Map<String, Object> sessionVariables,
      final Map<String, Object> requestProperties,
      final Context context,
      final WorkerExecutor workerExecutor,
      final ServiceContext serviceContext,
      final MetricsCallbackHolder metricsCallbackHolder) {
    // Must be run on worker as all this stuff is slow
    VertxUtils.checkIsWorker();

    final ConfiguredStatement<Query> statement = createStatement(
        sql, properties, sessionVariables);

    if (statement.getStatement().isPullQuery()) {
      final ImmutableAnalysis analysis = ksqlEngine
          .analyzeQueryWithNoOutputTopic(
              statement.getStatement(), statement.getStatementText(), properties);
      final DataSource dataSource = analysis.getFrom().getDataSource();
      final DataSource.DataSourceType dataSourceType = dataSource.getDataSourceType();
      Optional<ConsistencyOffsetVector> consistencyOffsetVector = Optional.empty();
      if (ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED)
          && requestProperties.containsKey(
              KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR)) {
        final String serializedCV = (String)requestProperties.get(
            KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR);
        // serializedCV will be null on the first request as the consistency vector is initialized
        // at the server
        consistencyOffsetVector = serializedCV != null
            ? Optional.of(ConsistencyOffsetVector.deserialize(serializedCV))
            : Optional.of(new ConsistencyOffsetVector());
      }
      switch (dataSourceType) {
        case KTABLE:
          return createTablePullQueryPublisher(
              analysis,
              context,
              serviceContext,
              statement,
              pullQueryMetrics,
              workerExecutor,
              metricsCallbackHolder,
              consistencyOffsetVector
          );
        case KSTREAM:
          return createStreamPullQueryPublisher(
              analysis,
              context,
              serviceContext,
              statement,
              workerExecutor,
              pullQueryMetrics,
              metricsCallbackHolder
          );
        default:
          throw new KsqlStatementException(
              "Unexpected data source type for pull query: " + dataSourceType,
              statement.getStatementText()
          );
      }
    } else if (ScalablePushUtil.isScalablePushQuery(statement.getStatement(), ksqlEngine,
        ksqlConfig, properties)) {
      final ImmutableAnalysis analysis = ksqlEngine
          .analyzeQueryWithNoOutputTopic(
              statement.getStatement(),
              statement.getStatementText(),
              properties
          );
      return createScalablePushQueryPublisher(
          analysis,
          context,
          serviceContext,
          statement,
          workerExecutor,
          requestProperties,
          metricsCallbackHolder,
          scalablePushQueryMetrics
      );
    } else {
      return createPushQueryPublisher(context, serviceContext, statement, workerExecutor);
    }
  }

  private QueryPublisher createScalablePushQueryPublisher(
      final ImmutableAnalysis analysis,
      final Context context,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final WorkerExecutor workerExecutor,
      final Map<String, Object> requestProperties,
      final MetricsCallbackHolder metricsCallbackHolder,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics

  ) {
    // First thing, set the metrics callback so that it gets called, even if we hit an error
    final AtomicReference<ScalablePushQueryMetadata> resultForMetrics =
        new AtomicReference<>(null);
    metricsCallbackHolder.setCallback(QueryMetricsUtil.initializeScalablePushMetricsCallback(
            scalablePushQueryMetrics, scalablePushBandRateLimiter, resultForMetrics));

    final BlockingQueryPublisher publisher =
        new BlockingQueryPublisher(context, workerExecutor);

    final PushQueryConfigRoutingOptions routingOptions =
        new PushQueryConfigRoutingOptions(requestProperties);

    final PushQueryConfigPlannerOptions plannerOptions = new PushQueryConfigPlannerOptions(
        ksqlConfig,
        statement.getSessionConfig().getOverrides());

    scalablePushBandRateLimiter.allow(KsqlQueryType.PUSH);

    final ScalablePushQueryMetadata query = ksqlEngine
        .executeScalablePushQuery(analysis, serviceContext, statement, pushRouting, routingOptions,
            plannerOptions, context, scalablePushQueryMetrics);
    query.prepare();
    resultForMetrics.set(query);

    publisher.setQueryHandle(
            new KsqlScalablePushQueryHandle(query, scalablePushQueryMetrics), false, true);

    return publisher;
  }

  private QueryPublisher createPushQueryPublisher(
      final Context context,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final WorkerExecutor workerExecutor
  ) {
    final BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);

    if (QueryCapacityUtil.exceedsPushQueryCapacity(ksqlEngine, ksqlRestConfig)) {
      QueryCapacityUtil.throwTooManyActivePushQueriesException(
              ksqlEngine,
              ksqlRestConfig,
              statement.getStatementText()
      );
    }

    final TransientQueryMetadata queryMetadata = ksqlEngine
        .executeTransientQuery(serviceContext, statement, true);

    localCommands.ifPresent(lc -> lc.write(queryMetadata));

    publisher.setQueryHandle(new KsqlQueryHandle(queryMetadata), false, false);

    return publisher;
  }

  private QueryPublisher createStreamPullQueryPublisher(
      final ImmutableAnalysis analysis,
      final Context context,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final WorkerExecutor workerExecutor,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final MetricsCallbackHolder metricsCallbackHolder
  ) {
    // First thing, set the metrics callback so that it gets called, even if we hit an error
    final AtomicReference<StreamPullQueryMetadata> resultForMetrics = new AtomicReference<>(null);
    final AtomicReference<Decrementer> refDecrementer = new AtomicReference<>(null);
    metricsCallbackHolder.setCallback(QueryMetricsUtil.initializePullStreamMetricsCallback(
        pullQueryMetrics, pullBandRateLimiter, analysis, resultForMetrics, refDecrementer));

    PullQueryExecutionUtil.checkRateLimit(rateLimiter);
    pullBandRateLimiter.allow(KsqlQueryType.PULL);
    refDecrementer.set(pullConcurrencyLimiter.increment());

    final BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);

    final StreamPullQueryMetadata metadata =
        ksqlEngine.createStreamPullQuery(
            serviceContext,
            analysis,
            statement,
            true
        );

    resultForMetrics.set(metadata);
    localCommands.ifPresent(lc -> lc.write(metadata.getTransientQueryMetadata()));

    publisher.setQueryHandle(
        new KsqlQueryHandle(metadata.getTransientQueryMetadata()),
        false,
        false
    );

    return publisher;
  }

  private QueryPublisher createTablePullQueryPublisher(
      final ImmutableAnalysis analysis,
      final Context context,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final WorkerExecutor workerExecutor,
      final MetricsCallbackHolder metricsCallbackHolder,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {
    // First thing, set the metrics callback so that it gets called, even if we hit an error
    final AtomicReference<PullQueryResult> resultForMetrics = new AtomicReference<>(null);
    metricsCallbackHolder.setCallback(QueryMetricsUtil.initializePullTableMetricsCallback(
        pullQueryMetrics, pullBandRateLimiter, resultForMetrics));
    final RoutingOptions routingOptions = new PullQueryConfigRoutingOptions(
        ksqlConfig,
        statement.getSessionConfig().getOverrides(),
        ImmutableMap.of()
    );

    final PullQueryConfigPlannerOptions plannerOptions = new PullQueryConfigPlannerOptions(
        ksqlConfig,
        statement.getSessionConfig().getOverrides()
    );

    Decrementer decrementer = null;
    try {
      PullQueryExecutionUtil.checkRateLimit(rateLimiter);
      decrementer = pullConcurrencyLimiter.increment();
      pullBandRateLimiter.allow(KsqlQueryType.PULL);
      final Decrementer finalDecrementer = decrementer;

      final PullQueryResult result = ksqlEngine.executeTablePullQuery(
          analysis,
          serviceContext,
          statement,
          routing,
          routingOptions,
          plannerOptions,
          pullQueryMetrics,
          false,
          consistencyOffsetVector
      );

      resultForMetrics.set(result);
      result.onCompletionOrException((v, throwable) -> {
        finalDecrementer.decrementAtMostOnce();
      });

      final BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);

      publisher.setQueryHandle(new KsqlPullQueryHandle(result, pullQueryMetrics), true, false);

      return publisher;
    } catch (Throwable t) {
      if (decrementer != null) {
        decrementer.decrementAtMostOnce();
      }
      throw t;
    }
  }

  private ConfiguredStatement<Query> createStatement(final String queryString,
      final Map<String, Object> properties, final Map<String, Object> sessionVariables) {
    final List<ParsedStatement> statements = ksqlEngine.parse(queryString);
    if ((statements.size() != 1)) {
      throw new KsqlStatementException(
          String
              .format("Expected exactly one KSQL statement; found %d instead", statements.size()),
          queryString);
    }
    final PreparedStatement<?> ps = ksqlEngine.prepare(
        statements.get(0),
        sessionVariables.entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()))
    );
    final Statement statement = ps.getStatement();
    if (!(statement instanceof Query)) {
      throw new KsqlStatementException("Not a query", queryString);
    }
    @SuppressWarnings("unchecked") final PreparedStatement<Query> psq =
        (PreparedStatement<Query>) ps;
    return ConfiguredStatement.of(psq, SessionConfig.of(ksqlConfig, properties));
  }

  private static List<String> colTypesFromSchema(final List<Column> columns) {
    return columns.stream()
        .map(Column::type)
        .map(type -> type.toString(FormatOptions.none()))
        .collect(Collectors.toList());
  }

  private static List<String> colNamesFromSchema(final List<Column> columns) {
    return columns.stream()
        .map(Column::name)
        .map(ColumnName::text)
        .collect(Collectors.toList());
  }

  private static class KsqlQueryHandle implements QueryHandle {

    private final PushQueryMetadata queryMetadata;

    KsqlQueryHandle(final PushQueryMetadata queryMetadata) {
      this.queryMetadata = Objects.requireNonNull(queryMetadata, "queryMetadata");
    }

    @Override
    public List<String> getColumnNames() {
      return colNamesFromSchema(queryMetadata.getLogicalSchema().value());
    }

    @Override
    public List<String> getColumnTypes() {
      return colTypesFromSchema(queryMetadata.getLogicalSchema().value());
    }

    @Override
    public void start() {
      queryMetadata.start();
    }

    @Override
    public void stop() {
      queryMetadata.close();
    }

    @Override
    public BlockingRowQueue getQueue() {
      return queryMetadata.getRowQueue();
    }

    @Override
    public void onException(final Consumer<Throwable> onException) {
      queryMetadata.setUncaughtExceptionHandler(throwable  -> {
        onException.accept(throwable);
        return null;
      });
    }

    @Override
    public QueryId getQueryId() {
      return queryMetadata.getQueryId();
    }

    @Override
    public Optional<ConsistencyOffsetVector> getConsistencyOffsetVector() {
      return Optional.empty();
    }
  }

  private static class KsqlPullQueryHandle implements QueryHandle {

    private final PullQueryResult result;
    private final Optional<PullQueryExecutorMetrics>  pullQueryMetrics;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    KsqlPullQueryHandle(final PullQueryResult result,
        final Optional<PullQueryExecutorMetrics> pullQueryMetrics) {
      this.result = Objects.requireNonNull(result);
      this.pullQueryMetrics = Objects.requireNonNull(pullQueryMetrics);
    }

    @Override
    public List<String> getColumnNames() {
      return colNamesFromSchema(result.getSchema().columns());
    }

    @Override
    public List<String> getColumnTypes() {
      return colTypesFromSchema(result.getSchema().columns());
    }

    @Override
    public void start() {
      try {
        result.start();
        result.onException(future::completeExceptionally);
        result.onCompletion(future::complete);
      } catch (Exception e) {
        pullQueryMetrics.ifPresent(metrics -> metrics.recordErrorRate(1, result.getSourceType(),
            result.getPlanType(), result.getRoutingNodeType()));
      }
    }

    @Override
    public void stop() {
      result.stop();
    }

    @Override
    public BlockingRowQueue getQueue() {
      return result.getPullQueryQueue();
    }

    @Override
    public void onException(final Consumer<Throwable> onException) {
      future.exceptionally(t -> {
        onException.accept(t);
        return null;
      });
    }

    @Override
    public QueryId getQueryId() {
      return result.getQueryId();
    }

    @Override
    public Optional<ConsistencyOffsetVector> getConsistencyOffsetVector() {
      return result.getConsistencyOffsetVector();
    }
  }

  private static class KsqlScalablePushQueryHandle implements QueryHandle {

    private final ScalablePushQueryMetadata scalablePushQueryMetadata;
    private final Optional<ScalablePushQueryMetrics>  scalablePushQueryMetrics;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    KsqlScalablePushQueryHandle(final ScalablePushQueryMetadata scalablePushQueryMetadata,
                        final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics) {
      this.scalablePushQueryMetadata = Objects.requireNonNull(scalablePushQueryMetadata);
      this.scalablePushQueryMetrics = Objects.requireNonNull(scalablePushQueryMetrics);
    }

    @Override
    public List<String> getColumnNames() {
      return colNamesFromSchema(scalablePushQueryMetadata.getLogicalSchema().columns());
    }

    @Override
    public List<String> getColumnTypes() {
      return colTypesFromSchema(scalablePushQueryMetadata.getLogicalSchema().columns());
    }

    @Override
    public void start() {
      try {
        scalablePushQueryMetadata.start();
        scalablePushQueryMetadata.onException(future::completeExceptionally);
        scalablePushQueryMetadata.onCompletion(future::complete);
      } catch (Exception e) {
        scalablePushQueryMetrics.ifPresent(metrics -> metrics.recordErrorRate(
                1,
                scalablePushQueryMetadata.getSourceType(),
                scalablePushQueryMetadata.getRoutingNodeType()));
      }
    }

    @Override
    public void stop() {
      scalablePushQueryMetadata.close();
    }

    @Override
    public BlockingRowQueue getQueue() {
      return scalablePushQueryMetadata.getRowQueue();
    }

    @Override
    public void onException(final Consumer<Throwable> onException) {
      future.exceptionally(t -> {
        onException.accept(t);
        return null;
      });
    }

    @Override
    public QueryId getQueryId() {
      return scalablePushQueryMetadata.getQueryId();
    }

    @Override
    public Optional<ConsistencyOffsetVector> getConsistencyOffsetVector() {
      return Optional.empty();
    }
  }
}
