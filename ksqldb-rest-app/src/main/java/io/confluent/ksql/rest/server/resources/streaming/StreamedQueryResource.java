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

import static java.util.Optional.empty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.TableRowsEntity;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.execution.PullQueryExecutorMetrics;
import io.confluent.ksql.rest.server.resources.KsqlConfigurable;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.rest.util.CommandStoreUtil;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private KsqlConfig ksqlConfig;
  private final PullQueryExecutor pullQueryExecutor;
  private Optional<PullQueryExecutorMetrics> pullQueryMetrics;
  private final Time time;

  public StreamedQueryResource(
      final KsqlEngine ksqlEngine,
      final CommandQueue commandQueue,
      final Duration disconnectCheckInterval,
      final Duration commandQueueCatchupTimeout,
      final ActivenessRegistrar activenessRegistrar,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler,
      final PullQueryExecutor pullQueryExecutor
  ) {
    this(
        ksqlEngine,
        new StatementParser(ksqlEngine),
        commandQueue,
        disconnectCheckInterval,
        commandQueueCatchupTimeout,
        activenessRegistrar,
        authorizationValidator,
        errorHandler,
        pullQueryExecutor
    );
  }

  @VisibleForTesting
  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  StreamedQueryResource(
      // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
      final KsqlEngine ksqlEngine,
      final StatementParser statementParser,
      final CommandQueue commandQueue,
      final Duration disconnectCheckInterval,
      final Duration commandQueueCatchupTimeout,
      final ActivenessRegistrar activenessRegistrar,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler,
      final PullQueryExecutor pullQueryExecutor
  ) {
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
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
    this.pullQueryExecutor = Objects.requireNonNull(pullQueryExecutor, "pullQueryExecutor");
    this.time = Time.SYSTEM;
  }

  @Override
  public void configure(final KsqlConfig config) {
    if (!config.getKsqlStreamConfigProps().containsKey(StreamsConfig.APPLICATION_SERVER_CONFIG)) {
      throw new IllegalArgumentException("Need KS application server set");
    }

    ksqlConfig = config;
    final Boolean collectMetrics = ksqlConfig.getBoolean(
        KsqlConfig.KSQL_QUERY_PULL_METRICS_ENABLED);
    this.pullQueryMetrics = collectMetrics
        ? Optional.of(new PullQueryExecutorMetrics(
        ksqlEngine.getServiceId(),
        ksqlConfig.getStringAsMap(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS)))
        : empty();
  }

  public EndpointResponse streamQuery(
      final KsqlSecurityContext securityContext,
      final KsqlRequest request,
      final CompletableFuture<Void> connectionClosedFuture,
      final Optional<Boolean> isInternalRequest
  ) {
    final long startTime = time.nanoseconds();
    throwIfNotConfigured();

    activenessRegistrar.updateLastRequestTime();

    final PreparedStatement<?> statement = parseStatement(request);

    CommandStoreUtil.httpWaitForCommandSequenceNumber(
        commandQueue, request, commandQueueCatchupTimeout);

    return handleStatement(securityContext, request, statement, startTime, connectionClosedFuture,
        isInternalRequest);
  }

  public void closeMetrics() {
    if (pullQueryMetrics != null) {
      pullQueryMetrics.ifPresent(PullQueryExecutorMetrics::close);
    }
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
      final long startTime,
      final CompletableFuture<Void> connectionClosedFuture,
      final Optional<Boolean> isInternalRequest
  ) {
    try {
      authorizationValidator.ifPresent(validator ->
          validator.checkAuthorization(
              securityContext,
              ksqlEngine.getMetaStore(),
              statement.getStatement())
      );

      if (statement.getStatement() instanceof Query) {
        final PreparedStatement<Query> queryStmt = (PreparedStatement<Query>) statement;

        if (queryStmt.getStatement().isPullQuery()) {
          final EndpointResponse response = handlePullQuery(
              securityContext.getServiceContext(),
              queryStmt,
              request.getConfigOverrides(),
              request.getRequestProperties(),
              isInternalRequest
          );
          if (pullQueryMetrics.isPresent()) {
            //Record latency at microsecond scale
            final double latency = (time.nanoseconds() - startTime) / 1000f;
            pullQueryMetrics.get().recordLatency(latency);
            pullQueryMetrics.get().recordRate(1);
          }
          return response;
        }

        return handlePushQuery(
            securityContext.getServiceContext(),
            queryStmt,
            request.getConfigOverrides(),
            connectionClosedFuture
        );
      }

      if (statement.getStatement() instanceof PrintTopic) {
        return handlePrintTopic(
            securityContext.getServiceContext(),
            request.getConfigOverrides(),
            (PreparedStatement<PrintTopic>) statement,
            connectionClosedFuture);
      }

      return Errors.badRequest(String.format(
          "Statement type `%s' not supported for this resource",
          statement.getClass().getName()));
    } catch (final TopicAuthorizationException e) {
      return errorHandler.accessDeniedFromKafkaResponse(e);
    } catch (final KsqlStatementException e) {
      return Errors.badStatement(e.getUnloggedMessage(), e.getSqlStatement());
    } catch (final KsqlException e) {
      return errorHandler.generateResponse(e, Errors.badRequest(e));
    }
  }

  private EndpointResponse handlePullQuery(
      final ServiceContext serviceContext,
      final PreparedStatement<Query> statement,
      final Map<String, Object> configOverrides,
      final Map<String, Object> requestProperties,
      final Optional<Boolean> isInternalRequest
  ) {
    final ConfiguredStatement<Query> configured =
        ConfiguredStatement.of(statement, configOverrides, requestProperties, ksqlConfig);

    final TableRowsEntity entity = pullQueryExecutor
        .execute(configured, serviceContext, pullQueryMetrics, isInternalRequest);

    final StreamedRow header = StreamedRow.header(entity.getQueryId(), entity.getSchema());

    final List<StreamedRow> rows = entity.getRows().stream()
        .map(StreamedQueryResource::toGenericRow)
        .map(StreamedRow::row)
        .collect(Collectors.toList());

    rows.add(0, header);

    final String data = rows.stream()
        .map(this::writeValueAsString)
        .collect(Collectors.joining("," + System.lineSeparator(), "[", "]"));

    return EndpointResponse.ok(data);
  }

  private EndpointResponse handlePushQuery(
      final ServiceContext serviceContext,
      final PreparedStatement<Query> statement,
      final Map<String, Object> streamsProperties,
      final CompletableFuture<Void> connectionClosedFuture
  ) {
    final ConfiguredStatement<Query> configured =
        ConfiguredStatement.of(statement, streamsProperties, ksqlConfig);

    final TransientQueryMetadata query = ksqlEngine.executeQuery(serviceContext, configured);

    final QueryStreamWriter queryStreamWriter = new QueryStreamWriter(
        query,
        disconnectCheckInterval.toMillis(),
        OBJECT_MAPPER,
        connectionClosedFuture
    );

    log.info("Streaming query '{}'", statement.getMaskedStatementText());
    return EndpointResponse.ok(queryStreamWriter);
  }

  private String writeValueAsString(final Object object) {
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


