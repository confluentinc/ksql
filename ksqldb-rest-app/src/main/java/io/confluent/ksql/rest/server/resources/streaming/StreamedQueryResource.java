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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.server.SlidingWindowRateLimiter;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.PullQueryExecutionUtil;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullSourceType;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.RoutingNodeType;
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
import io.confluent.ksql.rest.util.ScalablePushUtil;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
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
  private final RoutingFilterFactory routingFilterFactory;
  private final RateLimiter rateLimiter;
  private final ConcurrencyLimiter concurrencyLimiter;
  private final SlidingWindowRateLimiter pullBandRateLimiter;
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
      final RoutingFilterFactory routingFilterFactory,
      final RateLimiter rateLimiter,
      final ConcurrencyLimiter concurrencyLimiter,
      final SlidingWindowRateLimiter pullBandRateLimiter,
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
        routingFilterFactory,
        rateLimiter,
        concurrencyLimiter,
        pullBandRateLimiter,
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
      final RoutingFilterFactory routingFilterFactory,
      final RateLimiter rateLimiter,
      final ConcurrencyLimiter concurrencyLimiter,
      final SlidingWindowRateLimiter pullBandRateLimiter,
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
    this.routingFilterFactory =
        Objects.requireNonNull(routingFilterFactory, "routingFilterFactory");
    this.rateLimiter = Objects.requireNonNull(rateLimiter, "rateLimiter");
    this.concurrencyLimiter = Objects.requireNonNull(concurrencyLimiter, "concurrencyLimiter");
    this.pullBandRateLimiter = Objects.requireNonNull(pullBandRateLimiter, "pullBandRateLimiter");
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
        isInternalRequest, mediaType, metricsCallbackHolder, context, pullBandRateLimiter);
  }

  private void throwIfNotConfigured() {
    if (ksqlConfig == null) {
      throw new KsqlRestException(Errors.notReady());
    }
  }

  private PreparedStatement<?> parseStatement(final KsqlRequest request) {
    final String ksql = request.getUnmaskedKsql();
    if (ksql.trim().isEmpty()) {
      throw new KsqlRestException(Errors.badRequest("\"ksql\" field must be populated"));
    }

    try {
      return statementParser.parseSingleStatement(ksql);
    } catch (IllegalArgumentException | KsqlException e) {
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
      final SlidingWindowRateLimiter pullBandRateLimiter
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
        final PreparedStatement<Query> queryStmt = (PreparedStatement<Query>) statement;
        if (queryStmt.getStatement().isPullQuery()) {
          return handlePullQuery(
              securityContext.getServiceContext(),
              queryStmt,
              configProperties,
              request.getRequestProperties(),
              isInternalRequest,
              connectionClosedFuture,
              metricsCallbackHolder,
              pullBandRateLimiter
          );
        }

        // log validated statements for query anonymization
        QueryLogger.info("Transient query created", statement.getMaskedStatementText());

        if (ScalablePushUtil.isScalablePushQuery(statement.getStatement(), ksqlEngine, ksqlConfig,
            configProperties)) {
          return handleScalablePushQuery(
              securityContext.getServiceContext(),
              queryStmt,
              configProperties,
              request.getRequestProperties(),
              connectionClosedFuture,
              context
          );
        }

        return handlePushQuery(
            securityContext.getServiceContext(),
            queryStmt,
            configProperties,
            connectionClosedFuture,
            mediaType
        );
      }

      if (statement.getStatement() instanceof PrintTopic) {
        return handlePrintTopic(
            securityContext.getServiceContext(),
            configProperties,
            (PreparedStatement<PrintTopic>) statement,
            connectionClosedFuture);
      }

      return Errors.badRequest(String.format(
          "Statement type `%s' not supported for this resource",
          statement.getClass().getName()));
    } catch (final TopicAuthorizationException e) {
      return errorHandler.accessDeniedFromKafkaResponse(e);
    } catch (final KsqlStatementException e) {
      return Errors.badStatement(e.getRawUnloggedDetails(), e.getSqlStatement());
    } catch (final KsqlException e) {
      return errorHandler.generateResponse(e, Errors.badRequest(e));
    }
  }

  private EndpointResponse handlePullQuery(
      final ServiceContext serviceContext,
      final PreparedStatement<Query> statement,
      final Map<String, Object> configOverrides,
      final Map<String, Object> requestProperties,
      final Optional<Boolean> isInternalRequest,
      final CompletableFuture<Void> connectionClosedFuture,
      final MetricsCallbackHolder metricsCallbackHolder,
      final SlidingWindowRateLimiter pullBandRateLimiter
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

    final ConfiguredStatement<Query> configured = ConfiguredStatement
        .of(statement, SessionConfig.of(ksqlConfig, configOverrides));

    final SessionConfig sessionConfig = configured.getSessionConfig();
    if (!sessionConfig.getConfig(false)
        .getBoolean(KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG)) {
      throw new KsqlStatementException(
          "Pull queries are disabled."
              + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
              + System.lineSeparator()
              + "Please set " + KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG + "=true to enable "
              + "this feature."
              + System.lineSeparator(),
          statement.getMaskedStatementText());
    }

    final RoutingOptions routingOptions = new PullQueryConfigRoutingOptions(
        sessionConfig.getConfig(false),
        configured.getSessionConfig().getOverrides(),
        requestProperties
    );

    final PullQueryConfigPlannerOptions plannerOptions = new PullQueryConfigPlannerOptions(
        sessionConfig.getConfig(false),
        configured.getSessionConfig().getOverrides()
    );

    // A request is considered forwarded if the request has the forwarded flag or if the request
    // is from an internal listener.
    final boolean isAlreadyForwarded = routingOptions.getIsSkipForwardRequest()
        // Trust the forward request option if isInternalRequest isn't available.
        && isInternalRequest.orElse(true);

    // Only check the rate limit at the forwarding host
    Decrementer decrementer = null;
    if (!isAlreadyForwarded) {
      PullQueryExecutionUtil.checkRateLimit(rateLimiter);
      decrementer = concurrencyLimiter.increment();
    }
    pullBandRateLimiter.allow();

    final Optional<Decrementer> optionalDecrementer = Optional.ofNullable(decrementer);

    try {
      final PullQueryResult result = ksqlEngine.executePullQuery(
          serviceContext,
          configured,
          routing,
          routingOptions,
          plannerOptions,
          pullQueryMetrics,
          true
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
    } catch (Throwable t) {
      optionalDecrementer.ifPresent(Decrementer::decrementAtMostOnce);
      throw t;
    }
  }

  private EndpointResponse handleScalablePushQuery(
      final ServiceContext serviceContext,
      final PreparedStatement<Query> statement,
      final Map<String, Object> configOverrides,
      final Map<String, Object> requestProperties,
      final CompletableFuture<Void> connectionClosedFuture,
      final Context context
  ) {
    final ConfiguredStatement<Query> configured = ConfiguredStatement
        .of(statement, SessionConfig.of(ksqlConfig, configOverrides));

    final PushQueryConfigRoutingOptions routingOptions =
        new PushQueryConfigRoutingOptions(requestProperties);

    final PushQueryConfigPlannerOptions plannerOptions =
        new PushQueryConfigPlannerOptions(ksqlConfig, configOverrides);

    final ScalablePushQueryMetadata query = ksqlEngine
        .executeScalablePushQuery(serviceContext, configured, pushRouting, routingOptions,
            plannerOptions, context);


    final QueryStreamWriter queryStreamWriter = new QueryStreamWriter(
        query,
        disconnectCheckInterval.toMillis(),
        OBJECT_MAPPER,
        connectionClosedFuture
    );

    log.info("Streaming query '{}'", statement.getMaskedStatementText());
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
              statement.getMaskedStatementText()
      );
    }

    final TransientQueryMetadata query = ksqlEngine
        .executeQuery(serviceContext, configured, false);

    localCommands.ifPresent(lc -> lc.write(query));

    final QueryStreamWriter queryStreamWriter = new QueryStreamWriter(
        query,
        disconnectCheckInterval.toMillis(),
        OBJECT_MAPPER,
        connectionClosedFuture
    );

    log.info("Streaming query '{}'", statement.getMaskedStatementText());
    return EndpointResponse.ok(queryStreamWriter);
  }

  private static String writeValueAsString(final Object object) {
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
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
