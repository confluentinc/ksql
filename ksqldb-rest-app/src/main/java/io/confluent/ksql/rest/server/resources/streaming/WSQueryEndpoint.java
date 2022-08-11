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

import static io.netty.handler.codec.http.websocketx.WebSocketCloseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.websocketx.WebSocketCloseStatus.INVALID_MESSAGE_TYPE;
import static io.netty.handler.codec.http.websocketx.WebSocketCloseStatus.TRY_AGAIN_LATER;

import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.query.QueryExecutor;
import io.confluent.ksql.rest.server.query.QueryMetadataHolder;
import io.confluent.ksql.rest.util.CommandStoreUtil;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import io.vertx.core.Context;
import io.vertx.core.MultiMap;
import io.vertx.core.http.ServerWebSocket;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class WSQueryEndpoint {
  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private static final Logger log = LoggerFactory.getLogger(WSQueryEndpoint.class);

  private final KsqlConfig ksqlConfig;
  private final StatementParser statementParser;
  private final KsqlEngine ksqlEngine;
  private final CommandQueue commandQueue;
  private final ListeningScheduledExecutorService exec;
  private final ActivenessRegistrar activenessRegistrar;
  private final Duration commandQueueCatchupTimeout;
  private final Optional<KsqlAuthorizationValidator> authorizationValidator;
  private final Errors errorHandler;
  private final DenyListPropertyValidator denyListPropertyValidator;
  private final QueryExecutor queryExecutor;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public WSQueryEndpoint(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final KsqlConfig ksqlConfig,
      final StatementParser statementParser,
      final KsqlEngine ksqlEngine,
      final CommandQueue commandQueue,
      final ListeningScheduledExecutorService exec,
      final ActivenessRegistrar activenessRegistrar,
      final Duration commandQueueCatchupTimeout,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler,
      final DenyListPropertyValidator denyListPropertyValidator,
      final QueryExecutor queryExecutor
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.statementParser = Objects.requireNonNull(statementParser, "statementParser");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.commandQueue =
        Objects.requireNonNull(commandQueue, "commandQueue");
    this.exec = Objects.requireNonNull(exec, "exec");
    this.activenessRegistrar =
        Objects.requireNonNull(activenessRegistrar, "activenessRegistrar");
    this.commandQueueCatchupTimeout =
        Objects.requireNonNull(commandQueueCatchupTimeout, "commandQueueCatchupTimeout");
    this.authorizationValidator =
        Objects.requireNonNull(authorizationValidator, "authorizationValidator");
    this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");
    this.denyListPropertyValidator =
        Objects.requireNonNull(denyListPropertyValidator, "denyListPropertyValidator");
    this.queryExecutor = queryExecutor;
  }

  public void executeStreamQuery(final ServerWebSocket webSocket, final MultiMap requestParams,
      final KsqlSecurityContext ksqlSecurityContext, final Context context,
      final Optional<Long> timeout) {

    try {
      final long startTimeNanos = Time.SYSTEM.nanoseconds();
      if (timeout.isPresent()) {
        log.info("Setting websocket timeout to " + timeout.get() + " ms");
        exec.schedule(
            () -> SessionUtil.closeSilently(
                webSocket, INTERNAL_SERVER_ERROR.code(), "The request token has expired."),
            timeout.get(),
            TimeUnit.MILLISECONDS
        );
      }

      activenessRegistrar.updateLastRequestTime();

      validateVersion(requestParams);

      final KsqlRequest request = parseRequest(requestParams);

      try {
        CommandStoreUtil.waitForCommandSequenceNumber(commandQueue, request,
            commandQueueCatchupTimeout);
      } catch (final InterruptedException e) {
        log.debug("Interrupted while waiting for command queue "
                + "to reach specified command sequence number",
            e);
        SessionUtil.closeSilently(webSocket, INTERNAL_SERVER_ERROR.code(), e.getMessage());
        return;
      } catch (final TimeoutException e) {
        log.debug("Timeout while processing request", e);
        SessionUtil.closeSilently(webSocket, TRY_AGAIN_LATER.code(), e.getMessage());
        return;
      }

      final PreparedStatement<?> preparedStatement = parseStatement(request);

      final Statement statement = preparedStatement.getStatement();

      authorizationValidator.ifPresent(validator -> validator.checkAuthorization(
          ksqlSecurityContext,
          ksqlEngine.getMetaStore(),
          statement)
      );

      final RequestContext requestContext = new RequestContext(webSocket, request,
          ksqlSecurityContext);

      if (statement instanceof Query) {
        handleQuery(requestContext, (Query) statement, startTimeNanos, context);
      } else if (statement instanceof PrintTopic) {
        handlePrintTopic(requestContext, (PrintTopic) statement);
      } else {
        throw new IllegalArgumentException("Unexpected statement type " + statement);
      }

    } catch (final TopicAuthorizationException e) {
      log.debug("Error processing request", e);
      SessionUtil.closeSilently(
          webSocket,
          INVALID_MESSAGE_TYPE.code(),
          errorHandler.kafkaAuthorizationErrorMessage(e));
    } catch (final Exception e) {
      log.debug("Error processing request", e);
      SessionUtil.closeSilently(webSocket, INVALID_MESSAGE_TYPE.code(), e.getMessage());
    }
  }

  private static void validateVersion(final MultiMap requestParams) {
    final String version = requestParams.get("version");
    if (version == null) {
      return;
    }

    try {
      KsqlMediaType.valueOf("JSON", Integer.parseInt(version));
    } catch (final Exception e) {
      throw new IllegalArgumentException("Received invalid api version: " + version, e);
    }
  }

  private KsqlRequest parseRequest(final MultiMap requestParams) {
    try {
      final String jsonRequest = requestParams.get("request");

      if (jsonRequest == null || jsonRequest.isEmpty()) {
        throw new IllegalArgumentException("missing request parameter");
      }

      final KsqlRequest request = ApiJsonMapper.INSTANCE.get()
          .readValue(jsonRequest, KsqlRequest.class);
      if (request.getUnmaskedKsql().isEmpty()) {
        throw new IllegalArgumentException("\"ksql\" field of \"request\" must be populated");
      }
      // To validate props:
      denyListPropertyValidator.validateAll(request.getConfigOverrides());
      return request;
    } catch (final Exception e) {
      throw new IllegalArgumentException("Error parsing request: " + e.getMessage(), e);
    }
  }

  private PreparedStatement<?> parseStatement(final KsqlRequest request) {
    try {
      return statementParser.parseSingleStatement(request.getUnmaskedKsql());
    } catch (final Exception e) {
      throw new IllegalArgumentException("Error parsing query: " + e.getMessage(), e);
    }
  }

  private void attachCloseHandler(final ServerWebSocket websocket,
                                  final WebSocketSubscriber<?> subscriber) {
    websocket.closeHandler(v -> {
      if (subscriber != null) {
        subscriber.close();
        log.debug("Websocket {} closed, reason: {},  code: {}",
                websocket.textHandlerID(),
                websocket.closeReason(),
                websocket.closeStatusCode());
      }
    });
  }

  private void handleQuery(final RequestContext info, final Query query,
      final long startTimeNanos, final Context context) {
    final WebSocketSubscriber<StreamedRow> streamSubscriber =
        new WebSocketSubscriber<>(info.websocket);

    attachCloseHandler(info.websocket, streamSubscriber);

    final PreparedStatement<Query> statement = PreparedStatement.of(
        info.request.getUnmaskedKsql(), query);
    final MetricsCallbackHolder metricsCallbackHolder = new MetricsCallbackHolder();
    final QueryMetadataHolder queryMetadataHolder
        = queryExecutor.handleStatement(info.securityContext.getServiceContext(),
        info.request.getConfigOverrides(),
        info.request.getRequestProperties(),
        statement,
        Optional.empty(),
        metricsCallbackHolder,
        context,
        true);

    if (queryMetadataHolder.getPullQueryResult().isPresent()) {
      new PullQueryPublisher(
          exec,
          queryMetadataHolder.getPullQueryResult().get(),
          metricsCallbackHolder,
          startTimeNanos
      ).subscribe(streamSubscriber);
    } else if (queryMetadataHolder.getPushQueryMetadata().isPresent()) {
      new PushQueryPublisher(
          exec,
          queryMetadataHolder.getPushQueryMetadata().get(),
          metricsCallbackHolder,
          startTimeNanos
      ).subscribe(streamSubscriber);
    } else {
      throw new KsqlStatementException("Unknown query type", statement.getMaskedStatementText());
    }
  }

  private void handlePrintTopic(final RequestContext info, final PrintTopic printTopic) {
    final String topicName = printTopic.getTopic();

    if (!info.securityContext.getServiceContext().getTopicClient().isTopicExists(topicName)) {
      throw new IllegalArgumentException(
          "Topic does not exist, or KSQL does not have permission to list the topic: " + topicName);
    }

    final WebSocketSubscriber<String> topicSubscriber =
        new WebSocketSubscriber<>(info.websocket);

    attachCloseHandler(info.websocket, topicSubscriber);

    new PrintPublisher(exec, info.securityContext.getServiceContext(),
        ksqlConfig.getKsqlStreamConfigProps(), printTopic)
        .subscribe(topicSubscriber);
  }

  private static final class RequestContext {

    private final ServerWebSocket websocket;
    private final KsqlRequest request;
    private final KsqlSecurityContext securityContext;

    private RequestContext(
        final ServerWebSocket websocket,
        final KsqlRequest request,
        final KsqlSecurityContext securityContext
    ) {
      this.websocket = websocket;
      this.request = request;
      this.securityContext = securityContext;
    }
  }
}
