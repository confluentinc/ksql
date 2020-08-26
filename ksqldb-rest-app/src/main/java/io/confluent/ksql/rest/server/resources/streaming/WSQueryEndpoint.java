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
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.util.CommandStoreUtil;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import io.vertx.core.MultiMap;
import io.vertx.core.http.ServerWebSocket;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WSQueryEndpoint {

  private static final Logger log = LoggerFactory.getLogger(WSQueryEndpoint.class);

  private final KsqlConfig ksqlConfig;
  private final StatementParser statementParser;
  private final KsqlEngine ksqlEngine;
  private final CommandQueue commandQueue;
  private final ListeningScheduledExecutorService exec;
  private final ActivenessRegistrar activenessRegistrar;
  private final QueryPublisher pushQueryPublisher;
  private final IPullQueryPublisher pullQueryPublisher;
  private final PrintTopicPublisher topicPublisher;
  private final Duration commandQueueCatchupTimeout;
  private final Optional<KsqlAuthorizationValidator> authorizationValidator;
  private final Errors errorHandler;
  private final PullQueryExecutor pullQueryExecutor;
  private final DenyListPropertyValidator denyListPropertyValidator;

  private WebSocketSubscriber<?> subscriber;

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
      final PullQueryExecutor pullQueryExecutor,
      final DenyListPropertyValidator denyListPropertyValidator
  ) {
    this(ksqlConfig,
        statementParser,
        ksqlEngine,
        commandQueue,
        exec,
        WSQueryEndpoint::startPushQueryPublisher,
        WSQueryEndpoint::startPullQueryPublisher,
        WSQueryEndpoint::startPrintPublisher,
        activenessRegistrar,
        commandQueueCatchupTimeout,
        authorizationValidator,
        errorHandler,
        pullQueryExecutor,
        denyListPropertyValidator
    );
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  WSQueryEndpoint(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final KsqlConfig ksqlConfig,
      final StatementParser statementParser,
      final KsqlEngine ksqlEngine,
      final CommandQueue commandQueue,
      final ListeningScheduledExecutorService exec,
      final QueryPublisher pushQueryPublisher,
      final IPullQueryPublisher pullQueryPublisher,
      final PrintTopicPublisher topicPublisher,
      final ActivenessRegistrar activenessRegistrar,
      final Duration commandQueueCatchupTimeout,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler,
      final PullQueryExecutor pullQueryExecutor,
      final DenyListPropertyValidator denyListPropertyValidator
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.statementParser = Objects.requireNonNull(statementParser, "statementParser");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.commandQueue =
        Objects.requireNonNull(commandQueue, "commandQueue");
    this.exec = Objects.requireNonNull(exec, "exec");
    this.pushQueryPublisher = Objects.requireNonNull(pushQueryPublisher, "pushQueryPublisher");
    this.pullQueryPublisher = Objects.requireNonNull(pullQueryPublisher, "pullQueryPublisher");
    this.topicPublisher = Objects.requireNonNull(topicPublisher, "topicPublisher");
    this.activenessRegistrar =
        Objects.requireNonNull(activenessRegistrar, "activenessRegistrar");
    this.commandQueueCatchupTimeout =
        Objects.requireNonNull(commandQueueCatchupTimeout, "commandQueueCatchupTimeout");
    this.authorizationValidator =
        Objects.requireNonNull(authorizationValidator, "authorizationValidator");
    this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");
    this.pullQueryExecutor = Objects.requireNonNull(pullQueryExecutor, "pullQueryExecutor");
    this.denyListPropertyValidator =
        Objects.requireNonNull(denyListPropertyValidator, "denyListPropertyValidator");
  }

  public void executeStreamQuery(final ServerWebSocket webSocket, final MultiMap requestParams,
      final KsqlSecurityContext ksqlSecurityContext) {

    try {
      final long startTimeNanos = Time.SYSTEM.nanoseconds();

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
        handleQuery(requestContext, (Query) statement, startTimeNanos);
      } else if (statement instanceof PrintTopic) {
        handlePrintTopic(requestContext, (PrintTopic) statement);
      } else {
        throw new IllegalArgumentException("Unexpected statement type " + statement);
      }

      webSocket.closeHandler(v -> {
        if (subscriber != null) {
          subscriber.close();
        }
      });
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

  private void validateVersion(final MultiMap requestParams) {

    final String version = requestParams.get(Versions.KSQL_V1_WS_PARAM);

    if (version == null) {
      return;
    }

    if (!Versions.KSQL_V1_WS.equals(version)) {
      throw new IllegalArgumentException("Received invalid api version: " + version);
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
      if (request.getKsql().isEmpty()) {
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
      return statementParser.parseSingleStatement(request.getKsql());
    } catch (final Exception e) {
      throw new IllegalArgumentException("Error parsing query: " + e.getMessage(), e);
    }
  }

  private void handleQuery(final RequestContext info, final Query query,
      final long startTimeNanos) {
    final Map<String, Object> clientLocalProperties = info.request.getConfigOverrides();

    final WebSocketSubscriber<StreamedRow> streamSubscriber =
        new WebSocketSubscriber<>(info.websocket);
    this.subscriber = streamSubscriber;

    final PreparedStatement<Query> statement =
        PreparedStatement.of(info.request.getKsql(), query);

    final ConfiguredStatement<Query> configured =
        ConfiguredStatement.of(statement, clientLocalProperties, ksqlConfig);

    if (query.isPullQuery()) {
      pullQueryPublisher.start(
          ksqlEngine,
          info.securityContext.getServiceContext(),
          exec,
          configured,
          streamSubscriber,
          pullQueryExecutor,
          startTimeNanos
      );
    } else {
      pushQueryPublisher.start(
          ksqlEngine,
          info.securityContext.getServiceContext(),
          exec,
          configured,
          streamSubscriber
      );
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
    this.subscriber = topicSubscriber;

    topicPublisher.start(
        exec,
        info.securityContext.getServiceContext(),
        ksqlConfig.getKsqlStreamConfigProps(),
        printTopic,
        topicSubscriber
    );
  }

  private void handleUnsupportedStatement(
      final RequestContext ignored,
      final Statement statement
  ) {
    throw new IllegalArgumentException(String.format(
        "Statement type `%s' not supported for this resource",
        statement.getClass().getName()
    ));
  }

  private static void startPushQueryPublisher(
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ListeningScheduledExecutorService exec,
      final ConfiguredStatement<Query> query,
      final WebSocketSubscriber<StreamedRow> streamSubscriber
  ) {
    new PushQueryPublisher(ksqlEngine, serviceContext, exec, query)
        .subscribe(streamSubscriber);
  }

  private static void startPullQueryPublisher(
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ListeningScheduledExecutorService ignored,
      final ConfiguredStatement<Query> query,
      final WebSocketSubscriber<StreamedRow> streamSubscriber,
      final PullQueryExecutor pullQueryExecutor,
      final long startTimeNanos
  ) {
    new PullQueryPublisher(
        serviceContext,
        query,
        pullQueryExecutor,
        startTimeNanos
    ).subscribe(streamSubscriber);
  }

  private static void startPrintPublisher(
      final ListeningScheduledExecutorService exec,
      final ServiceContext serviceContext,
      final Map<String, Object> ksqlStreamConfigProps,
      final PrintTopic printTopic,
      final WebSocketSubscriber<String> topicSubscriber
  ) {
    new PrintPublisher(exec, serviceContext, ksqlStreamConfigProps, printTopic)
        .subscribe(topicSubscriber);
  }

  interface QueryPublisher {

    void start(
        KsqlEngine ksqlEngine,
        ServiceContext serviceContext,
        ListeningScheduledExecutorService exec,
        ConfiguredStatement<Query> query,
        WebSocketSubscriber<StreamedRow> subscriber);

  }

  interface IPullQueryPublisher {

    void start(
        KsqlEngine ksqlEngine,
        ServiceContext serviceContext,
        ListeningScheduledExecutorService exec,
        ConfiguredStatement<Query> query,
        WebSocketSubscriber<StreamedRow> subscriber,
        PullQueryExecutor pullQueryExecutor,
        long startTimeNanos);

  }

  interface PrintTopicPublisher {

    void start(
        ListeningScheduledExecutorService exec,
        ServiceContext serviceContext,
        Map<String, Object> consumerProperties,
        PrintTopic printTopic,
        WebSocketSubscriber<String> subscriber);
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
