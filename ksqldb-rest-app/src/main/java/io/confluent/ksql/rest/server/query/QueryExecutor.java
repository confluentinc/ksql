/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.query;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.server.SlidingWindowRateLimiter;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.pull.HARouting;
import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.execution.scalablepush.PushRouting;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
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
import io.confluent.ksql.rest.util.RateLimiter;
import io.confluent.ksql.rest.util.ScalablePushUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.StreamPullQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.vertx.core.Context;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link QueryExecutor} is responsible for executing query statement types, namely push and pull
 * queries. This consolidates launching, certain error handling, and rate limiting all in one
 * place. It's still up to the endpoints to take the {@link QueryMetadataHolder}, inspect it to
 * find the type of query launched, then start and processes the output of the query.
 */
public class QueryExecutor {

  private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);

  private final KsqlExecutionContext ksqlEngine;
  private final KsqlRestConfig ksqlRestConfig;
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;
  private final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics;
  private final RateLimiter rateLimiter;
  private final ConcurrencyLimiter concurrencyLimiter;
  private final SlidingWindowRateLimiter pullBandRateLimiter;
  private final SlidingWindowRateLimiter scalablePushBandRateLimiter;
  private final HARouting routing;
  private final PushRouting pushRouting;
  private final Optional<LocalCommands> localCommands;

  @SuppressWarnings("ParameterNumber")
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public QueryExecutor(
      final KsqlEngine ksqlEngine,
      final KsqlRestConfig ksqlRestConfig,
      final KsqlConfig ksqlConfig,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
      final RateLimiter rateLimiter,
      final ConcurrencyLimiter concurrencyLimiter,
      final SlidingWindowRateLimiter pullBandRateLimiter,
      final SlidingWindowRateLimiter scalablePushBandRateLimiter,
      final HARouting routing,
      final PushRouting pushRouting,
      final Optional<LocalCommands> localCommands
  ) {
    this.ksqlEngine = ksqlEngine;
    this.ksqlRestConfig = ksqlRestConfig;
    this.pullQueryMetrics = pullQueryMetrics;
    this.scalablePushQueryMetrics = scalablePushQueryMetrics;
    this.rateLimiter = rateLimiter;
    this.concurrencyLimiter = concurrencyLimiter;
    this.pullBandRateLimiter = pullBandRateLimiter;
    this.scalablePushBandRateLimiter = scalablePushBandRateLimiter;
    this.routing = routing;
    this.pushRouting = pushRouting;
    this.localCommands = localCommands;
  }

  @SuppressWarnings("unchecked")
  public QueryMetadataHolder handleStatement(
      final ServiceContext serviceContext,
      final Map<String, Object> configOverrides,
      final Map<String, Object> requestProperties,
      final PreparedStatement<?> statement,
      final Optional<Boolean> isInternalRequest,
      final MetricsCallbackHolder metricsCallbackHolder,
      final Context context,
      final boolean excludeTombstones
  ) {

    if (statement.getStatement() instanceof Query) {
      return handleQuery(
          serviceContext,
          (PreparedStatement<Query>) statement,
          isInternalRequest,
          metricsCallbackHolder,
          configOverrides,
          requestProperties,
          context,
          excludeTombstones
      );
    } else {
      return QueryMetadataHolder.unhandled();
    }
  }


  private QueryMetadataHolder handleQuery(final ServiceContext serviceContext,
      final PreparedStatement<Query> statement,
      final Optional<Boolean> isInternalRequest,
      final MetricsCallbackHolder metricsCallbackHolder,
      final Map<String, Object> configOverrides,
      final Map<String, Object> requestProperties,
      final Context context,
      final boolean excludeTombstones
  ) {
    if (statement.getStatement().isPullQuery()) {
      final ImmutableAnalysis analysis = ksqlEngine
          .analyzeQueryWithNoOutputTopic(
              statement.getStatement(), statement.getMaskedStatementText(), configOverrides);
      final DataSource dataSource = analysis.getFrom().getDataSource();
      final DataSource.DataSourceType dataSourceType = dataSource.getDataSourceType();

      if (!ksqlEngine.getKsqlConfig().getBoolean(KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG)) {
        throw new KsqlStatementException(
            "Pull queries are disabled."
                + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
                + System.lineSeparator()
                + "Please set " + KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG + "=true to enable "
                + "this feature."
                + System.lineSeparator(),
            statement.getMaskedStatementText());
      }

      switch (dataSourceType) {
        case KTABLE: {
          // First thing, set the metrics callback so that it gets called, even if we hit an error
          final AtomicReference<PullQueryResult> resultForMetrics = new AtomicReference<>(null);
          metricsCallbackHolder.setCallback(QueryMetricsUtil.initializePullTableMetricsCallback(
              pullQueryMetrics, pullBandRateLimiter, resultForMetrics));

          final SessionConfig sessionConfig
              = SessionConfig.of(ksqlEngine.getKsqlConfig(), configOverrides);
          final ConfiguredStatement<Query> configured = ConfiguredStatement
              .of(statement, sessionConfig);

          return handleTablePullQuery(
              analysis,
              serviceContext,
              configured,
              requestProperties,
              isInternalRequest,
              pullBandRateLimiter,
              resultForMetrics,
              Optional.empty()
          );
        }
        case KSTREAM: {
          // First thing, set the metrics callback so that it gets called, even if we hit an error
          final AtomicReference<StreamPullQueryMetadata> resultForMetrics =
              new AtomicReference<>(null);
          final AtomicReference<Decrementer> refDecrementer = new AtomicReference<>(null);
          metricsCallbackHolder.setCallback(
              QueryMetricsUtil.initializePullStreamMetricsCallback(
                  pullQueryMetrics, pullBandRateLimiter, analysis, resultForMetrics,
                  refDecrementer));

          final SessionConfig sessionConfig
              = SessionConfig.of(ksqlEngine.getKsqlConfig(), configOverrides);
          final ConfiguredStatement<Query> configured = ConfiguredStatement
              .of(statement, sessionConfig);

          return handleStreamPullQuery(
              analysis,
              serviceContext,
              configured,
              resultForMetrics,
              refDecrementer
          );
        }
        default:
          throw new KsqlStatementException(
              "Unexpected data source type for pull query: " + dataSourceType,
              statement.getMaskedStatementText()
          );
      }
    } else if (ScalablePushUtil
        .isScalablePushQuery(statement.getStatement(), ksqlEngine, ksqlEngine.getKsqlConfig(),
            configOverrides)) {
      // First thing, set the metrics callback so that it gets called, even if we hit an error
      final AtomicReference<ScalablePushQueryMetadata> resultForMetrics =
          new AtomicReference<>(null);
      metricsCallbackHolder.setCallback(QueryMetricsUtil.initializeScalablePushMetricsCallback(
          scalablePushQueryMetrics, scalablePushBandRateLimiter, resultForMetrics));

      final ImmutableAnalysis analysis = ksqlEngine
          .analyzeQueryWithNoOutputTopic(
              statement.getStatement(),
              statement.getMaskedStatementText(),
              configOverrides
          );

      QueryLogger.info("Scalable push query created", statement.getMaskedStatementText());

      return handleScalablePushQuery(
          analysis,
          serviceContext,
          statement,
          configOverrides,
          requestProperties,
          context,
          scalablePushBandRateLimiter,
          resultForMetrics
      );
    } else {
      // log validated statements for query anonymization
      QueryLogger.info("Transient query created", statement.getMaskedStatementText());
      return handlePushQuery(
          serviceContext,
          statement,
          configOverrides,
          excludeTombstones
      );
    }
  }

  private QueryMetadataHolder handleTablePullQuery(
      final ImmutableAnalysis analysis,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> configured,
      final Map<String, Object> requestProperties,
      final Optional<Boolean> isInternalRequest,
      final SlidingWindowRateLimiter pullBandRateLimiter,
      final AtomicReference<PullQueryResult> resultForMetrics,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector) {

    final RoutingOptions routingOptions = new PullQueryConfigRoutingOptions(
        configured.getSessionConfig().getConfig(false),
        configured.getSessionConfig().getOverrides(),
        requestProperties
    );

    final PullQueryConfigPlannerOptions plannerOptions = new PullQueryConfigPlannerOptions(
        configured.getSessionConfig().getConfig(false),
        configured.getSessionConfig().getOverrides()
    );

    // A request is considered forwarded if the request has the forwarded flag or if the request
    // is from an internal listener.
    final boolean isAlreadyForwarded = routingOptions.getIsSkipForwardRequest()
        // Trust the forward request option if isInternalRequest isn't available.
        && isInternalRequest.orElse(true);

    // Only check the rate limit at the forwarding host
    Decrementer decrementer = null;
    try {
      if (!isAlreadyForwarded) {
        rateLimiter.checkLimit();
        decrementer = concurrencyLimiter.increment();
      }
      pullBandRateLimiter.allow(KsqlQueryType.PULL);

      final Optional<Decrementer> optionalDecrementer = Optional.ofNullable(decrementer);
      final PullQueryResult result = ksqlEngine.executeTablePullQuery(
          analysis,
          serviceContext,
          configured,
          routing,
          routingOptions,
          plannerOptions,
          pullQueryMetrics,
          false,
          consistencyOffsetVector
      );
      resultForMetrics.set(result);
      result.onCompletionOrException(
          (v, t) -> optionalDecrementer.ifPresent(Decrementer::decrementAtMostOnce)
      );

      return QueryMetadataHolder.of(result);
    } catch (final Throwable t) {
      if (decrementer != null) {
        decrementer.decrementAtMostOnce();
      }
      throw t;
    }
  }

  private QueryMetadataHolder handleScalablePushQuery(
      final ImmutableAnalysis analysis,
      final ServiceContext serviceContext,
      final PreparedStatement<Query> statement,
      final Map<String, Object> configOverrides,
      final Map<String, Object> requestProperties,
      final Context context,
      final SlidingWindowRateLimiter scalablePushBandRateLimiter,
      final AtomicReference<ScalablePushQueryMetadata> resultForMetrics
  ) {
    final ConfiguredStatement<Query> configured = ConfiguredStatement
        .of(statement, SessionConfig.of(ksqlEngine.getKsqlConfig(), configOverrides));

    final PushQueryConfigRoutingOptions routingOptions = new PushQueryConfigRoutingOptions(
        ksqlEngine.getKsqlConfig(),
        configOverrides,
        requestProperties
    );

    final PushQueryConfigPlannerOptions plannerOptions =
        new PushQueryConfigPlannerOptions(ksqlEngine.getKsqlConfig(), configOverrides);

    scalablePushBandRateLimiter.allow(KsqlQueryType.PUSH);

    final ScalablePushQueryMetadata query = ksqlEngine
        .executeScalablePushQuery(analysis, serviceContext, configured, pushRouting, routingOptions,
            plannerOptions, context, scalablePushQueryMetrics);
    query.prepare();
    resultForMetrics.set(query);

    QueryLogger.info("Streaming scalable push query", statement.getMaskedStatementText());
    return QueryMetadataHolder.of(query);
  }

  private QueryMetadataHolder handleStreamPullQuery(
      final ImmutableAnalysis analysis,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> configured,
      final AtomicReference<StreamPullQueryMetadata> resultForMetrics,
      final AtomicReference<Decrementer> refDecrementer) {

    // Apply the same rate, bandwidth and concurrency limits as with table pull queries
    rateLimiter.checkLimit();
    pullBandRateLimiter.allow(KsqlQueryType.PULL);
    refDecrementer.set(concurrencyLimiter.increment());

    final StreamPullQueryMetadata streamPullQueryMetadata = ksqlEngine
        .createStreamPullQuery(
            serviceContext,
            analysis,
            configured,
            false
        );
    resultForMetrics.set(streamPullQueryMetadata);
    localCommands.ifPresent(lc -> lc.write(streamPullQueryMetadata.getTransientQueryMetadata()));

    return QueryMetadataHolder.of(streamPullQueryMetadata);
  }

  private QueryMetadataHolder handlePushQuery(
      final ServiceContext serviceContext,
      final PreparedStatement<Query> statement,
      final Map<String, Object> streamsProperties,
      final boolean excludeTombstones
  ) {
    final ConfiguredStatement<Query> configured = ConfiguredStatement
        .of(statement, SessionConfig.of(ksqlEngine.getKsqlConfig(), streamsProperties));

    if (QueryCapacityUtil.exceedsPushQueryCapacity(ksqlEngine, ksqlRestConfig)) {
      QueryCapacityUtil.throwTooManyActivePushQueriesException(
          ksqlEngine,
          ksqlRestConfig,
          statement.getMaskedStatementText()
      );
    }

    final TransientQueryMetadata query = ksqlEngine
        .executeTransientQuery(serviceContext, configured, excludeTombstones);

    localCommands.ifPresent(lc -> lc.write(query));

    QueryLogger.info("Streaming query", statement.getMaskedStatementText());
    return QueryMetadataHolder.of(query);
  }
}
