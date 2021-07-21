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
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullSourceType;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.RoutingNodeType;
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
import io.confluent.ksql.rest.util.ScalablePushUtil;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PushQueryMetadata;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
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
  private final RateLimiter rateLimiter;
  private final ConcurrencyLimiter pullConcurrencyLimiter;
  private final SlidingWindowRateLimiter pullBandRateLimiter;
  private final HARouting routing;
  private final PushRouting pushRouting;
  private final Optional<LocalCommands> localCommands;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public QueryEndpoint(
      // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final KsqlRestConfig ksqlRestConfig,
      final RoutingFilterFactory routingFilterFactory,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final RateLimiter rateLimiter,
      final ConcurrencyLimiter pullConcurrencyLimiter,
      final SlidingWindowRateLimiter pullBandLimiter,
      final HARouting routing,
      final PushRouting pushRouting,
      final Optional<LocalCommands> localCommands
  ) {
    this.ksqlEngine = ksqlEngine;
    this.ksqlConfig = ksqlConfig;
    this.ksqlRestConfig = ksqlRestConfig;
    this.routingFilterFactory = routingFilterFactory;
    this.pullQueryMetrics = pullQueryMetrics;
    this.rateLimiter = rateLimiter;
    this.pullConcurrencyLimiter = pullConcurrencyLimiter;
    this.pullBandRateLimiter = pullBandLimiter;
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
      return createPullQueryPublisher(
          context, serviceContext, statement, pullQueryMetrics, workerExecutor,
          metricsCallbackHolder);
    } else if (ScalablePushUtil.isScalablePushQuery(statement.getStatement(), ksqlEngine,
        ksqlConfig, properties)) {
      return createScalablePushQueryPublisher(context, serviceContext, statement, workerExecutor,
          requestProperties);
    } else {
      return createPushQueryPublisher(context, serviceContext, statement, workerExecutor);
    }
  }

  private QueryPublisher createScalablePushQueryPublisher(
      final Context context,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final WorkerExecutor workerExecutor,
      final Map<String, Object> requestProperties
  ) {
    final BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);

    final PushQueryConfigRoutingOptions routingOptions =
        new PushQueryConfigRoutingOptions(requestProperties);

    final PushQueryConfigPlannerOptions plannerOptions = new PushQueryConfigPlannerOptions(
        ksqlConfig,
        statement.getSessionConfig().getOverrides());

    final ScalablePushQueryMetadata query = ksqlEngine
        .executeScalablePushQuery(serviceContext, statement, pushRouting, routingOptions,
            plannerOptions, context);


    publisher.setQueryHandle(new KsqlQueryHandle(query), false, true);

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
        .executeQuery(serviceContext, statement, true);

    localCommands.ifPresent(lc -> lc.write(queryMetadata));

    publisher.setQueryHandle(new KsqlQueryHandle(queryMetadata), false, false);

    return publisher;
  }

  private QueryPublisher createPullQueryPublisher(
      final Context context,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final WorkerExecutor workerExecutor,
      final MetricsCallbackHolder metricsCallbackHolder
  ) {
    // First thing, set the metrics callback so that it gets called, even if we hit an error
    final AtomicReference<PullQueryResult> resultForMetrics = new AtomicReference<>(null);
    metricsCallbackHolder.setCallback((statusCode, requestBytes, responseBytes, startTimeNanos) -> {
      pullQueryMetrics.ifPresent(metrics -> {
        metrics.recordStatusCode(statusCode);
        metrics.recordRequestSize(requestBytes);
        final PullQueryResult r = resultForMetrics.get();
        final PullSourceType sourceType = Optional.ofNullable(r).map(
            PullQueryResult::getSourceType).orElse(PullSourceType.UNKNOWN);
        final PullPhysicalPlanType planType = Optional.ofNullable(r).map(
            PullQueryResult::getPlanType).orElse(PullPhysicalPlanType.UNKNOWN);
        final RoutingNodeType routingNodeType = Optional.ofNullable(r).map(
            PullQueryResult::getRoutingNodeType).orElse(RoutingNodeType.UNKNOWN);
        metrics.recordResponseSize(responseBytes, sourceType, planType, routingNodeType);
        metrics.recordLatency(startTimeNanos, sourceType, planType, routingNodeType);
        metrics.recordRowsReturned(
            Optional.ofNullable(r).map(PullQueryResult::getTotalRowsReturned).orElse(0L),
            sourceType, planType, routingNodeType);
        metrics.recordRowsProcessed(
            Optional.ofNullable(r).map(PullQueryResult::getTotalRowsProcessed).orElse(0L),
            sourceType, planType, routingNodeType);
        pullBandRateLimiter.add(responseBytes);
      });
    });

    final RoutingOptions routingOptions = new PullQueryConfigRoutingOptions(
        ksqlConfig,
        statement.getSessionConfig().getOverrides(),
        ImmutableMap.of()
    );

    final PullQueryConfigPlannerOptions plannerOptions = new PullQueryConfigPlannerOptions(
        ksqlConfig,
        statement.getSessionConfig().getOverrides()
    );

    PullQueryExecutionUtil.checkRateLimit(rateLimiter);
    final Decrementer decrementer = pullConcurrencyLimiter.increment();
    pullBandRateLimiter.allow();

    try {
      final PullQueryResult result = ksqlEngine.executePullQuery(
          serviceContext,
          statement,
          routing,
          routingOptions,
          plannerOptions,
          pullQueryMetrics,
          false
      );

      resultForMetrics.set(result);
      result.onCompletionOrException((v, throwable) -> {
        decrementer.decrementAtMostOnce();
      });

      final BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);

      publisher.setQueryHandle(new KsqlPullQueryHandle(result, pullQueryMetrics), true, false);

      return publisher;
    } catch (Throwable t) {
      decrementer.decrementAtMostOnce();
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
      queryMetadata.setUncaughtExceptionHandler(throwable -> {
        System.out.println("Blah ");
        return null;
      });
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
  }
}
