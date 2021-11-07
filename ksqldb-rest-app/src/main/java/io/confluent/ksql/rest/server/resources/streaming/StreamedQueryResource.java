/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.server.SlidingWindowRateLimiter;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.PullQueryExecutionUtil;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.physical.scalablepush.PushRouting;
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.LocalCommands;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.resources.KsqlConfigurable;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.rest.util.CommandStoreUtil;
import io.confluent.ksql.rest.util.ConcurrencyLimiter;
import io.confluent.ksql.rest.util.ConcurrencyLimiter.Decrementer;
import io.confluent.ksql.rest.util.QueryCapacityUtil;
import io.confluent.ksql.rest.util.QueryMetricsUtil;
import io.confluent.ksql.rest.util.ScalablePushUtil;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.StreamPullQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import io.vertx.core.Context;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"ClassDataAbstractionCoupling"})
public class StreamedQueryResource implements KsqlConfigurable {

  private static final Logger log = LoggerFactory.getLogger(StreamedQueryResource.class);

  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  private final KsqlEngine ksqlEngine;
  private final StatementParser statementParser;
  private final CommandQueue commandQueue;
  private final Duration disconnectCheckInterval;
  private final Duration commandQueueCatchupTimeout;
  private final ActivenessRegistrar activenessRegistrar;
  private final Optional<KsqlAuthorizationValidator> authorizationValidator;
  private final Errors errorHandler;
  private final DenyListPropertyValidator denyListPropertyValidator;
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;
  private final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics;
  private final RoutingFilterFactory routingFilterFactory;
  private final RateLimiter rateLimiter;
  private final ConcurrencyLimiter concurrencyLimiter;
  private final SlidingWindowRateLimiter pullBandRateLimiter;
  private final SlidingWindowRateLimiter scalablePushBandRateLimiter;
  private final HARouting routing;
  private final PushRouting pushRouting;
  private final Optional<LocalCommands> localCommands;

  private KsqlConfig ksqlConfig;
  private KsqlRestConfig ksqlRestConfig;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public StreamedQueryResource(
      final KsqlEngine ksqlEngine,
      final KsqlRestConfig ksqlRestConfig,
      final CommandQueue commandQueue,
      final Duration disconnectCheckInterval,
      final Duration commandQueueCatchupTimeout,
      final ActivenessRegistrar activenessRegistrar,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler,
      final DenyListPropertyValidator denyListPropertyValidator,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
      final RoutingFilterFactory routingFilterFactory,
      final RateLimiter rateLimiter,
      final ConcurrencyLimiter concurrencyLimiter,
      final SlidingWindowRateLimiter pullBandRateLimiter,
      final SlidingWindowRateLimiter scalablePushBandRateLimiter,
      final HARouting routing,
      final PushRouting pushRouting,
      final Optional<LocalCommands> localCommands
  ) {
    this(
        ksqlEngine,
        ksqlRestConfig,
        new StatementParser(ksqlEngine),
        commandQueue,
        disconnectCheckInterval,
        commandQueueCatchupTimeout,
        activenessRegistrar,
        authorizationValidator,
        errorHandler,
        denyListPropertyValidator,
        pullQueryMetrics,
        scalablePushQueryMetrics,
        routingFilterFactory,
        rateLimiter,
        concurrencyLimiter,
        pullBandRateLimiter,
        scalablePushBandRateLimiter,
        routing,
        pushRouting,
        localCommands
    );
  }

  @VisibleForTesting
  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  StreamedQueryResource(
      // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
      final KsqlEngine ksqlEngine,
      final KsqlRestConfig ksqlRestConfig,
      final StatementParser statementParser,
      final CommandQueue commandQueue,
      final Duration disconnectCheckInterval,
      final Duration commandQueueCatchupTimeout,
      final ActivenessRegistrar activenessRegistrar,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler,
      final DenyListPropertyValidator denyListPropertyValidator,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
      final RoutingFilterFactory routingFilterFactory,
      final RateLimiter rateLimiter,
      final ConcurrencyLimiter concurrencyLimiter,
      final SlidingWindowRateLimiter pullBandRateLimiter,
      final SlidingWindowRateLimiter scalablePushBandRateLimiter,
      final HARouting routing,
      final PushRouting pushRouting,
      final Optional<LocalCommands> localCommands
  ) {
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.ksqlRestConfig = Objects.requireNonNull(ksqlRestConfig, "ksqlRestConfig");
    this.statementParser = Objects.requireNonNull(statementParser, "statementParser");
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.disconnectCheckInterval =
        Objects.requireNonNull(disconnectCheckInterval, "disconnectCheckInterval");
    this.commandQueueCatchupTimeout =
        Objects.requireNonNull(commandQueueCatchupTimeout, "commandQueueCatchupTimeout");
    this.activenessRegistrar =
        Objects.requireNonNull(activenessRegistrar, "activenessRegistrar");
    this.authorizationValidator = authorizationValidator;
    this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");
    this.denyListPropertyValidator =
        Objects.requireNonNull(denyListPropertyValidator, "denyListPropertyValidator");
    this.pullQueryMetrics = Objects.requireNonNull(pullQueryMetrics, "pullQueryMetrics");
    this.scalablePushQueryMetrics =
        Objects.requireNonNull(scalablePushQueryMetrics, "scalablePushQueryMetrics");
    this.routingFilterFactory =
        Objects.requireNonNull(routingFilterFactory, "routingFilterFactory");
    this.rateLimiter = Objects.requireNonNull(rateLimiter, "rateLimiter");
    this.concurrencyLimiter = Objects.requireNonNull(concurrencyLimiter, "concurrencyLimiter");
    this.pullBandRateLimiter = Objects.requireNonNull(pullBandRateLimiter, "pullBandRateLimiter");
    this.scalablePushBandRateLimiter =
        Objects.requireNonNull(scalablePushBandRateLimiter, "scalablePushBandRateLimiter");
    this.routing = Objects.requireNonNull(routing, "routing");
    this.pushRouting = pushRouting;
    this.localCommands = Objects.requireNonNull(localCommands, "localCommands");
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public void configure(final KsqlConfig config) {
    if (!config.getKsqlStreamConfigProps().containsKey(StreamsConfig.APPLICATION_SERVER_CONFIG)) {
      throw new IllegalArgumentException("Need KS application server set");
    }

    ksqlConfig = config;
  }

  public EndpointResponse streamQuery(
      final KsqlSecurityContext securityContext,
      final KsqlRequest request,
      final CompletableFuture<Void> connectionClosedFuture,
      final Optional<Boolean> isInternalRequest,
      final KsqlMediaType mediaType,
      final MetricsCallbackHolder metricsCallbackHolder,
      final Context context
  ) {
    throwIfNotConfigured();
    activenessRegistrar.updateLastRequestTime();

    final PreparedStatement<?> statement = parseStatement(request);

    CommandStoreUtil.httpWaitForCommandSequenceNumber(
        commandQueue, request, commandQueueCatchupTimeout);

    return handleStatement(securityContext, request, statement, connectionClosedFuture,
        isInternalRequest, mediaType, metricsCallbackHolder, context, pullBandRateLimiter,
        scalablePushBandRateLimiter);
  }

  private void throwIfNotConfigured() {
    if (ksqlConfig == null) {
      throw new KsqlRestException(Errors.notReady());
    }
  }

  private PreparedStatement<?> parseStatement(final KsqlRequest request) {
    final String ksql = request.getKsql();
    if (ksql.trim().isEmpty()) {
      throw new KsqlRestException(Errors.badRequest("\"ksql\" field must be populated"));
    }

    try {
      return statementParser.parseSingleStatement(ksql);
    } catch (final IllegalArgumentException | KsqlException e) {
      throw new KsqlRestException(Errors.badStatement(e, ksql));
    }
  }

  @SuppressWarnings("unchecked")
  private EndpointResponse handleStatement(
      final KsqlSecurityContext securityContext,
      final KsqlRequest request,
      final PreparedStatement<?> statement,
      final CompletableFuture<Void> connectionClosedFuture,
      final Optional<Boolean> isInternalRequest,
      final KsqlMediaType mediaType,
      final MetricsCallbackHolder metricsCallbackHolder,
      final Context context,
      final SlidingWindowRateLimiter pullBandRateLimiter,
      final SlidingWindowRateLimiter scalablePushBandRateLimiter
  ) {
    try {
      authorizationValidator.ifPresent(validator ->
          validator.checkAuthorization(
              securityContext,
              ksqlEngine.getMetaStore(),
              statement.getStatement())
      );

      final Map<String, Object> configProperties = request.getConfigOverrides();
      denyListPropertyValidator.validateAll(configProperties);

      if (statement.getStatement() instanceof Query) {
        return handleQuery(
            securityContext,
            request,
            (PreparedStatement<Query>) statement,
            connectionClosedFuture,
            mediaType,
            isInternalRequest,
            metricsCallbackHolder,
            configProperties,
            context,
            pullBandRateLimiter,
            scalablePushBandRateLimiter
        );
      } else if (statement.getStatement() instanceof PrintTopic) {
        return handlePrintTopic(
            securityContext.getServiceContext(),
            configProperties,
            (PreparedStatement<PrintTopic>) statement,
            connectionClosedFuture);
      } else {
        return Errors.badRequest(String.format(
            "Statement type `%s' not supported for this resource",
            statement.getClass().getName()));
      }
    } catch (final TopicAuthorizationException e) {
      return errorHandler.accessDeniedFromKafkaResponse(e);
    } catch (final KsqlStatementException e) {
      return Errors.badStatement(e.getRawMessage(), e.getSqlStatement());
    } catch (final KsqlException e) {
      return errorHandler.generateResponse(e, Errors.badRequest(e));
    }
  }

  // CHECKSTYLE_RULES.OFF: MethodLength
  // CHECKSTYLE_RULES.OFF: JavaNCSS
  private EndpointResponse handleQuery(final KsqlSecurityContext securityContext,
      // CHECKSTYLE_RULES.ON: MethodLength
      // CHECKSTYLE_RULES.ON: JavaNCSS
      final KsqlRequest request,
      final PreparedStatement<Query> statement,
      final CompletableFuture<Void> connectionClosedFuture,
      final KsqlMediaType mediaType,
      final Optional<Boolean> isInternalRequest,
      final MetricsCallbackHolder metricsCallbackHolder,
      final Map<String, Object> configProperties,
      final Context context,
      final SlidingWindowRateLimiter pullBandRateLimiter,
      final SlidingWindowRateLimiter scalablePushBandRateLimiter) {

    if (statement.getStatement().isPullQuery()) {
      final ImmutableAnalysis analysis = ksqlEngine
          .analyzeQueryWithNoOutputTopic(
              statement.getStatement(), statement.getStatementText(), configProperties);
      final DataSource dataSource = analysis.getFrom().getDataSource();
      final DataSource.DataSourceType dataSourceType = dataSource.getDataSourceType();

      if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG)) {
        throw new KsqlStatementException(
            "Pull queries are disabled."
                + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
                + System.lineSeparator()
                + "Please set " + KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG + "=true to enable "
                + "this feature."
                + System.lineSeparator(),
            statement.getStatementText());
      }

      switch (dataSourceType) {
        case KTABLE: {
          // First thing, set the metrics callback so that it gets called, even if we hit an error
          final AtomicReference<PullQueryResult> resultForMetrics = new AtomicReference<>(null);
          metricsCallbackHolder.setCallback(QueryMetricsUtil.initializePullTableMetricsCallback(
              pullQueryMetrics, pullBandRateLimiter, resultForMetrics));

          final SessionConfig sessionConfig = SessionConfig.of(ksqlConfig, configProperties);
          final ConfiguredStatement<Query> configured = ConfiguredStatement
              .of(statement, sessionConfig);

          return handleTablePullQuery(
              analysis,
              securityContext.getServiceContext(),
              configured,
              request.getRequestProperties(),
              isInternalRequest,
              connectionClosedFuture,
              pullBandRateLimiter,
              resultForMetrics
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

          final SessionConfig sessionConfig = SessionConfig.of(ksqlConfig, configProperties);
          final ConfiguredStatement<Query> configured = ConfiguredStatement
              .of(statement, sessionConfig);

          return handleStreamPullQuery(
              analysis,
              securityContext.getServiceContext(),
              configured,
              connectionClosedFuture,
              resultForMetrics,
              refDecrementer
          );
        }
        default:
          throw new KsqlStatementException(
              "Unexpected data source type for pull query: " + dataSourceType,
              statement.getStatementText()
          );
      }
    } else if (ScalablePushUtil
        .isScalablePushQuery(statement.getStatement(), ksqlEngine, ksqlConfig,
            configProperties)) {
      // First thing, set the metrics callback so that it gets called, even if we hit an error
      final AtomicReference<ScalablePushQueryMetadata> resultForMetrics =
              new AtomicReference<>(null);
      metricsCallbackHolder.setCallback(QueryMetricsUtil.initializeScalablePushMetricsCallback(
              scalablePushQueryMetrics, scalablePushBandRateLimiter, resultForMetrics));

      final ImmutableAnalysis analysis = ksqlEngine
          .analyzeQueryWithNoOutputTopic(
              statement.getStatement(),
              statement.getStatementText(),
              configProperties
          );

      QueryLogger.info("Scalable push query created", statement.getStatementText());

      return handleScalablePushQuery(
          analysis,
          securityContext.getServiceContext(),
          statement,
          configProperties,
          request.getRequestProperties(),
          connectionClosedFuture,
          context,
          scalablePushBandRateLimiter,
          resultForMetrics
      );
    } else {
      // log validated statements for query anonymization
      QueryLogger.info("Transient query created", statement.getStatementText());
      return handlePushQuery(
          securityContext.getServiceContext(),
          statement,
          configProperties,
          connectionClosedFuture,
          mediaType
      );
    }
  }

  private EndpointResponse handleTablePullQuery(
      final ImmutableAnalysis analysis,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> configured,
      final Map<String, Object> requestProperties,
      final Optional<Boolean> isInternalRequest,
      final CompletableFuture<Void> connectionClosedFuture,
      final SlidingWindowRateLimiter pullBandRateLimiter,
      final AtomicReference<PullQueryResult> resultForMetrics) {

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
        PullQueryExecutionUtil.checkRateLimit(rateLimiter);
        decrementer = concurrencyLimiter.increment();
      }
      pullBandRateLimiter.allow(KsqlQueryType.PULL);

      final Optional<Decrementer> optionalDecrementer = Optional.ofNullable(decrementer);
      Optional<ConsistencyOffsetVector> consistencyOffsetVector = Optional.empty();
      if (ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED)
          && requestProperties.containsKey(
              KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR)) {
        final String serializedCV = (String)requestProperties.get(
            KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR);
        // serializedCV will be empty on the first request as the consistency vector is initialized
        // at the server
        consistencyOffsetVector = !serializedCV.equals("")
            ? Optional.of(ConsistencyOffsetVector.deserialize(serializedCV))
            : Optional.of(new ConsistencyOffsetVector());
      }
      final PullQueryResult result = ksqlEngine.executeTablePullQuery(
          analysis,
          serviceContext,
          configured,
          routing,
          routingOptions,
          plannerOptions,
          pullQueryMetrics,
          true,
          consistencyOffsetVector
      );
      resultForMetrics.set(result);
      result.onCompletionOrException(
          (v, t) -> optionalDecrementer.ifPresent(Decrementer::decrementAtMostOnce)
      );

      final PullQueryStreamWriter pullQueryStreamWriter = new PullQueryStreamWriter(
          result,
          disconnectCheckInterval.toMillis(),
          OBJECT_MAPPER,
          result.getPullQueryQueue(),
          Clock.systemUTC(),
          connectionClosedFuture);

      return EndpointResponse.ok(pullQueryStreamWriter);
    } catch (final Throwable t) {
      if (decrementer != null) {
        decrementer.decrementAtMostOnce();
      }
      throw t;
    }
  }

  private EndpointResponse handleScalablePushQuery(
      final ImmutableAnalysis analysis,
      final ServiceContext serviceContext,
      final PreparedStatement<Query> statement,
      final Map<String, Object> configOverrides,
      final Map<String, Object> requestProperties,
      final CompletableFuture<Void> connectionClosedFuture,
      final Context context,
      final SlidingWindowRateLimiter scalablePushBandRateLimiter,
      final AtomicReference<ScalablePushQueryMetadata> resultForMetrics
  ) {
    final ConfiguredStatement<Query> configured = ConfiguredStatement
        .of(statement, SessionConfig.of(ksqlConfig, configOverrides));

    final PushQueryConfigRoutingOptions routingOptions =
        new PushQueryConfigRoutingOptions(requestProperties);

    final PushQueryConfigPlannerOptions plannerOptions =
        new PushQueryConfigPlannerOptions(ksqlConfig, configOverrides);

    scalablePushBandRateLimiter.allow(KsqlQueryType.PUSH);

    final ScalablePushQueryMetadata query = ksqlEngine
        .executeScalablePushQuery(analysis, serviceContext, configured, pushRouting, routingOptions,
            plannerOptions, context, scalablePushQueryMetrics);
    query.prepare();
    resultForMetrics.set(query);

    final QueryStreamWriter queryStreamWriter = new QueryStreamWriter(
        query,
        disconnectCheckInterval.toMillis(),
        OBJECT_MAPPER,
        connectionClosedFuture
    );

    QueryLogger.info("Streaming scalable push query", statement.getStatementText());
    return EndpointResponse.ok(queryStreamWriter);
  }

  private EndpointResponse handleStreamPullQuery(
      final ImmutableAnalysis analysis,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> configured,
      final CompletableFuture<Void> connectionClosedFuture,
      final AtomicReference<StreamPullQueryMetadata> resultForMetrics,
      final AtomicReference<Decrementer> refDecrementer) {

    // Apply the same rate, bandwidth and concurrency limits as with table pull queries
    PullQueryExecutionUtil.checkRateLimit(rateLimiter);
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

    final QueryStreamWriter queryStreamWriter = new QueryStreamWriter(
        streamPullQueryMetadata.getTransientQueryMetadata(),
        disconnectCheckInterval.toMillis(),
        OBJECT_MAPPER,
        connectionClosedFuture,
        streamPullQueryMetadata.getEndOffsets().isEmpty()
    );

    return EndpointResponse.ok(queryStreamWriter);
  }

  private EndpointResponse handlePushQuery(
      final ServiceContext serviceContext,
      final PreparedStatement<Query> statement,
      final Map<String, Object> streamsProperties,
      final CompletableFuture<Void> connectionClosedFuture,
      final KsqlMediaType mediaType
  ) {
    final ConfiguredStatement<Query> configured = ConfiguredStatement
        .of(statement, SessionConfig.of(ksqlConfig, streamsProperties));

    if (QueryCapacityUtil.exceedsPushQueryCapacity(ksqlEngine, ksqlRestConfig)) {
      QueryCapacityUtil.throwTooManyActivePushQueriesException(
          ksqlEngine,
          ksqlRestConfig,
          statement.getStatementText()
      );
    }

    final TransientQueryMetadata query = ksqlEngine
        .executeTransientQuery(serviceContext, configured, false);

    localCommands.ifPresent(lc -> lc.write(query));

    final QueryStreamWriter queryStreamWriter = new QueryStreamWriter(
        query,
        disconnectCheckInterval.toMillis(),
        OBJECT_MAPPER,
        connectionClosedFuture
    );

    log.info("Streaming query '{}'", statement.getStatementText());
    return EndpointResponse.ok(queryStreamWriter);
  }

  private EndpointResponse handlePrintTopic(
      final ServiceContext serviceContext,
      final Map<String, Object> streamProperties,
      final PreparedStatement<PrintTopic> statement,
      final CompletableFuture<Void> connectionClosedFuture
  ) {
    final PrintTopic printTopic = statement.getStatement();
    final String topicName = printTopic.getTopic();

    if (!serviceContext.getTopicClient().isTopicExists(topicName)) {
      final Collection<String> possibleAlternatives =
          findPossibleTopicMatches(topicName, serviceContext);

      final String reverseSuggestion = possibleAlternatives.isEmpty()
          ? ""
          : possibleAlternatives.stream()
              .map(name -> "\tprint " + name + ";")
              .collect(Collectors.joining(
                  System.lineSeparator(),
                  System.lineSeparator() + "Did you mean:" + System.lineSeparator(),
                  ""
              ));

      throw new KsqlRestException(
          Errors.badRequest(
              "Could not find topic '" + topicName + "', "
                  + "or the KSQL user does not have permissions to list the topic. "
                  + "Topic names are case-sensitive."
                  + reverseSuggestion
          ));
    }

    final Map<String, Object> propertiesWithOverrides =
        new HashMap<>(ksqlConfig.getKsqlStreamConfigProps());
    propertiesWithOverrides.putAll(streamProperties);

    final TopicStreamWriter topicStreamWriter = TopicStreamWriter.create(
        serviceContext,
        propertiesWithOverrides,
        printTopic,
        disconnectCheckInterval,
        connectionClosedFuture
    );

    log.info("Printing topic '{}'", topicName);
    return EndpointResponse.ok(topicStreamWriter);
  }

  private static Collection<String> findPossibleTopicMatches(
      final String topicName,
      final ServiceContext serviceContext
  ) {
    return serviceContext.getTopicClient().listTopicNames().stream()
        .filter(name -> name.equalsIgnoreCase(topicName))
        .collect(Collectors.toSet());
  }

  private static GenericRow toGenericRow(final List<?> values) {
    return new GenericRow().appendAll(values);
  }
}
