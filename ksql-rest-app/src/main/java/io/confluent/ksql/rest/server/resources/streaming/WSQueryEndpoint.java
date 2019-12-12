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
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.DefaultServiceContextFactory;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.UserServiceContextFactory;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.CommandStoreUtil;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.HandlerMaps;
import io.confluent.ksql.util.HandlerMaps.ClassHandlerMap2;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.security.Principal;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.EndpointConfig;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import javax.ws.rs.core.Response;

import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnstableApiUsage")
@ServerEndpoint(value = "/query")
public class WSQueryEndpoint {

  private static final Logger log = LoggerFactory.getLogger(WSQueryEndpoint.class);

  private static final ClassHandlerMap2<Statement, WSQueryEndpoint, RequestContext> HANDLER_MAP =
      HandlerMaps
          .forClass(Statement.class)
          .withArgTypes(WSQueryEndpoint.class, RequestContext.class)
          .put(Query.class, WSQueryEndpoint::handleQuery)
          .put(PrintTopic.class, WSQueryEndpoint::handlePrintTopic)
          .build();

  private final KsqlConfig ksqlConfig;
  private final ObjectMapper mapper;
  private final StatementParser statementParser;
  private final KsqlEngine ksqlEngine;
  private final CommandQueue commandQueue;
  private final ListeningScheduledExecutorService exec;
  private final ActivenessRegistrar activenessRegistrar;
  private final QueryPublisher pushQueryPublisher;
  private final QueryPublisher pullQueryPublisher;
  private final PrintTopicPublisher topicPublisher;
  private final Duration commandQueueCatchupTimeout;
  private final Optional<KsqlAuthorizationValidator> authorizationValidator;
  private final KsqlSecurityExtension securityExtension;
  private final UserServiceContextFactory serviceContextFactory;
  private final DefaultServiceContextFactory defaultServiceContextFactory;
  private final ServerState serverState;
  private final Errors errorHandler;

  private WebSocketSubscriber<?> subscriber;
  private ServiceContext serviceContext;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public WSQueryEndpoint(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final KsqlConfig ksqlConfig,
      final ObjectMapper mapper,
      final StatementParser statementParser,
      final KsqlEngine ksqlEngine,
      final CommandQueue commandQueue,
      final ListeningScheduledExecutorService exec,
      final ActivenessRegistrar activenessRegistrar,
      final Duration commandQueueCatchupTimeout,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler,
      final KsqlSecurityExtension securityExtension,
      final ServerState serverState
  ) {
    this(ksqlConfig,
        mapper,
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
        securityExtension,
        RestServiceContextFactory::create,
        RestServiceContextFactory::create,
        serverState);
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  WSQueryEndpoint(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final KsqlConfig ksqlConfig,
      final ObjectMapper mapper,
      final StatementParser statementParser,
      final KsqlEngine ksqlEngine,
      final CommandQueue commandQueue,
      final ListeningScheduledExecutorService exec,
      final QueryPublisher pushQueryPublisher,
      final QueryPublisher pullQueryPublisher,
      final PrintTopicPublisher topicPublisher,
      final ActivenessRegistrar activenessRegistrar,
      final Duration commandQueueCatchupTimeout,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final Errors errorHandler,
      final KsqlSecurityExtension securityExtension,
      final UserServiceContextFactory serviceContextFactory,
      final DefaultServiceContextFactory defaultServiceContextFactory,
      final ServerState serverState
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
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
    this.securityExtension = Objects.requireNonNull(securityExtension, "securityExtension");
    this.serviceContextFactory =
        Objects.requireNonNull(serviceContextFactory, "serviceContextFactory");
    this.defaultServiceContextFactory =
        Objects.requireNonNull(defaultServiceContextFactory, "defaultServiceContextFactory");
    this.serverState = Objects.requireNonNull(serverState, "serverState");
    this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");;
  }

  @SuppressWarnings("unused")
  @OnOpen
  public void onOpen(final Session session, final EndpointConfig unused) {
    log.debug("Opening websocket session {}", session.getId());

    try {
      // Check if the user has authorization to open a WS session
      checkAuthorization(session);

      validateVersion(session);

      final Optional<Response> readyResponse = serverState.checkReady();
      if (readyResponse.isPresent()) {
        final String msg = ((KsqlErrorMessage) readyResponse.get().getEntity()).getMessage();
        SessionUtil.closeSilently(session, CloseCodes.TRY_AGAIN_LATER, msg);
        return;
      }

      final KsqlRequest request = parseRequest(session);

      try {
        CommandStoreUtil.waitForCommandSequenceNumber(commandQueue, request,
            commandQueueCatchupTimeout);
      } catch (final InterruptedException e) {
        log.debug("Interrupted while waiting for command queue "
            + "to reach specified command sequence number",
            e);
        SessionUtil.closeSilently(session, CloseCodes.UNEXPECTED_CONDITION, e.getMessage());
        return;
      } catch (final TimeoutException e) {
        log.debug("Timeout while processing request", e);
        SessionUtil.closeSilently(session, CloseCodes.TRY_AGAIN_LATER, e.getMessage());
        return;
      }

      final PreparedStatement<?> preparedStatement = parseStatement(request);

      serviceContext = createServiceContext(session.getUserPrincipal());

      final Statement statement = preparedStatement.getStatement();
      final Class<? extends Statement> type = statement.getClass();

      validateKafkaAuthorization(statement);

      HANDLER_MAP
          .getOrDefault(type, WSQueryEndpoint::handleUnsupportedStatement)
          .handle(this, new RequestContext(session, request, serviceContext), statement);
    } catch (final TopicAuthorizationException e) {
      log.debug("Error processing request", e);
      SessionUtil.closeSilently(
          session, CloseCodes.CANNOT_ACCEPT, errorHandler.webSocketAuthorizationErrorMessage(e));
    } catch (final Exception e) {
      log.debug("Error processing request", e);
      SessionUtil.closeSilently(session, CloseCodes.CANNOT_ACCEPT, e.getMessage());
    }
  }

  @OnClose
  public void onClose(final Session session, final CloseReason closeReason) {
    if (subscriber != null) {
      subscriber.close();
    }

    if (serviceContext != null) {
      serviceContext.close();
    }

    log.debug(
        "Closing websocket session {} ({}): {}",
        session.getId(),
        closeReason.getCloseCode(),
        closeReason.getReasonPhrase()
    );
  }

  @SuppressWarnings("MethodMayBeStatic")
  @OnError
  public void onError(final Session session, final Throwable t) {
    log.error("websocket error in session {}", session.getId(), t);
    SessionUtil.closeSilently(session, CloseCodes.UNEXPECTED_CONDITION, t.getMessage());
  }

  private void checkAuthorization(final Session session) {
    final String method = "POST";
    final String path = this.getClass().getAnnotation(ServerEndpoint.class).value();
    final Principal user = session.getUserPrincipal();

    securityExtension.getAuthorizationProvider().ifPresent(
        provider -> {
          try {
            provider.checkEndpointAccess(user, method, path);
          } catch (final Throwable t) {
            log.warn(String.format("User:%s is denied access to Websocket "
                + "%s endpoint", user.getName(), path), t);
            throw new KsqlException(t);
          }
        }
    );
  }

  private ServiceContext createServiceContext(final Principal principal) {
    // Creates a ServiceContext using the user's credentials, so the WS query topics are
    // accessed with the user permission context (defaults to KSQL service context)

    if (!securityExtension.getUserContextProvider().isPresent()) {
      return defaultServiceContextFactory.create(ksqlConfig, Optional.empty());
    }

    return securityExtension.getUserContextProvider()
        .map(provider ->
            serviceContextFactory.create(
                ksqlConfig,
                Optional.empty(),
                provider.getKafkaClientSupplier(principal),
                provider.getSchemaRegistryClientFactory(principal)))
        .get();
  }

  private void validateVersion(final Session session) {
    final Map<String, List<String>> parameters = session.getRequestParameterMap();
    activenessRegistrar.updateLastRequestTime();

    final List<String> versionParam = parameters.getOrDefault(
        Versions.KSQL_V1_WS_PARAM, Collections.singletonList(Versions.KSQL_V1_WS));

    if (versionParam.isEmpty()) {
      return;
    }

    if (versionParam.size() != 1) {
      throw new IllegalArgumentException("Received multiple api versions: " + versionParam);
    }

    if (!versionParam.get(0).equals(Versions.KSQL_V1_WS)) {
      throw new IllegalArgumentException("Received invalid api version: " + versionParam);
    }
  }

  private KsqlRequest parseRequest(final Session session) {
    try {
      final List<String> jsonRequests = session.getRequestParameterMap()
          .getOrDefault("request", Collections.emptyList());

      if (jsonRequests == null || jsonRequests.isEmpty()) {
        throw new IllegalArgumentException("missing request parameter");
      }

      final String jsonRequest = Iterables.getLast(jsonRequests, "");
      if (jsonRequest == null || jsonRequest.isEmpty()) {
        throw new IllegalArgumentException("request parameter empty");
      }

      final KsqlRequest request = mapper.readValue(jsonRequest, KsqlRequest.class);
      if (request.getKsql().isEmpty()) {
        throw new IllegalArgumentException("\"ksql\" field of \"request\" must be populated");
      }
      // To validate props:
      request.getStreamsProperties();
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

  private void validateKafkaAuthorization(final Statement statement) {
    if (statement instanceof Query && ((Query) statement).isPullQuery()) {
      final boolean skipAccessValidation = ksqlConfig.getBoolean(
          KsqlConfig.KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_CONFIG);
      if (authorizationValidator.isPresent() && !skipAccessValidation) {
        throw new KsqlException("Pull queries are not currently supported when "
            + "access validation against Kafka is configured. If you really want to "
            + "bypass this limitation please set "
            + KsqlConfig.KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_CONFIG + "=true "
            + KsqlConfig.KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_DOC);
      }
    } else {
      authorizationValidator.ifPresent(validator -> validator.checkAuthorization(
          serviceContext,
          ksqlEngine.getMetaStore(),
          statement)
      );
    }
  }


  @SuppressWarnings({"unused"})
  private void handleQuery(final RequestContext info, final Query query) {
    final Map<String, Object> clientLocalProperties = info.request.getStreamsProperties();

    final WebSocketSubscriber<StreamedRow> streamSubscriber =
        new WebSocketSubscriber<>(info.session, mapper);
    this.subscriber = streamSubscriber;

    final PreparedStatement<Query> statement =
        PreparedStatement.of(info.request.getKsql(), query);

    final ConfiguredStatement<Query> configured =
        ConfiguredStatement.of(statement, clientLocalProperties, ksqlConfig);

    final QueryPublisher queryPublisher = query.isPullQuery()
        ? pullQueryPublisher
        : pushQueryPublisher;

    queryPublisher.start(
        ksqlEngine,
        info.serviceContext,
        exec,
        configured,
        streamSubscriber
    );
  }

  private void handlePrintTopic(final RequestContext info, final PrintTopic printTopic) {
    final String topicName = printTopic.getTopic();

    if (!info.serviceContext.getTopicClient().isTopicExists(topicName)) {
      throw new IllegalArgumentException(
          "Topic does not exist, or KSQL does not have permission to list the topic: " + topicName);
    }

    final WebSocketSubscriber<String> topicSubscriber =
        new WebSocketSubscriber<>(info.session, mapper);
    this.subscriber = topicSubscriber;

    topicPublisher.start(
        exec,
        info.serviceContext,
        ksqlConfig.getKsqlStreamConfigProps(),
        printTopic,
        topicSubscriber
    );
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
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
      final WebSocketSubscriber<StreamedRow> streamSubscriber
  ) {
    new PullQueryPublisher(ksqlEngine, serviceContext, query)
        .subscribe(streamSubscriber);
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

  interface PrintTopicPublisher {

    void start(
        ListeningScheduledExecutorService exec,
        ServiceContext serviceContext,
        Map<String, Object> consumerProperties,
        PrintTopic printTopic,
        WebSocketSubscriber<String> subscriber);
  }

  private static final class RequestContext {

    private final Session session;
    private final KsqlRequest request;
    private final ServiceContext serviceContext;

    private RequestContext(
        final Session session, final KsqlRequest request, final ServiceContext serviceContext) {
      this.session = session;
      this.request = request;
      this.serviceContext = serviceContext;
    }
  }
}
