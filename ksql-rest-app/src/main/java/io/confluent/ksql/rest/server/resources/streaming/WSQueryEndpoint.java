/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.util.CommandStoreUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.HandlerMaps;
import io.confluent.ksql.util.HandlerMaps.ClassHandlerMap2;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.EndpointConfig;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ServerEndpoint(value = "/query")
public class WSQueryEndpoint {

  private static final Logger log = LoggerFactory.getLogger(WSQueryEndpoint.class);

  private static final ClassHandlerMap2<Statement, WSQueryEndpoint, SessionAndRequest> HANDLER_MAP =
      HandlerMaps
          .forClass(Statement.class)
          .withArgTypes(WSQueryEndpoint.class, SessionAndRequest.class)
          .put(Query.class, WSQueryEndpoint::handleQuery)
          .put(PrintTopic.class, WSQueryEndpoint::handlePrintTopic)
          .build();

  private final KsqlConfig ksqlConfig;
  private final ObjectMapper mapper;
  private final StatementParser statementParser;
  private final KsqlEngine ksqlEngine;
  private final ServiceContext serviceContext;
  private final CommandQueue commandQueue;
  private final ListeningScheduledExecutorService exec;
  private final ActivenessRegistrar activenessRegistrar;
  private final QueryPublisher queryPublisher;
  private final PrintTopicPublisher topicPublisher;
  private final Duration commandQueueCatchupTimeout;

  private WebSocketSubscriber<?> subscriber;

  public WSQueryEndpoint(
      final KsqlConfig ksqlConfig,
      final ObjectMapper mapper,
      final StatementParser statementParser,
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final CommandQueue commandQueue,
      final ListeningScheduledExecutorService exec,
      final ActivenessRegistrar activenessRegistrar,
      final Duration commandQueueCatchupTimeout
  ) {
    this(ksqlConfig,
        mapper,
        statementParser,
        ksqlEngine,
        serviceContext,
        commandQueue,
        exec,
        WSQueryEndpoint::startQueryPublisher,
        WSQueryEndpoint::startPrintPublisher,
        activenessRegistrar,
        commandQueueCatchupTimeout);
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  WSQueryEndpoint(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final KsqlConfig ksqlConfig,
      final ObjectMapper mapper,
      final StatementParser statementParser,
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final CommandQueue commandQueue,
      final ListeningScheduledExecutorService exec,
      final QueryPublisher queryPublisher,
      final PrintTopicPublisher topicPublisher,
      final ActivenessRegistrar activenessRegistrar,
      final Duration commandQueueCatchupTimeout
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.statementParser = Objects.requireNonNull(statementParser, "statementParser");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.commandQueue =
        Objects.requireNonNull(commandQueue, "commandQueue");
    this.exec = Objects.requireNonNull(exec, "exec");
    this.queryPublisher = Objects.requireNonNull(queryPublisher, "queryPublisher");
    this.topicPublisher = Objects.requireNonNull(topicPublisher, "topicPublisher");
    this.activenessRegistrar =
        Objects.requireNonNull(activenessRegistrar, "activenessRegistrar");
    this.commandQueueCatchupTimeout =
        Objects.requireNonNull(commandQueueCatchupTimeout, "commandQueueCatchupTimeout");
  }

  @SuppressWarnings("unused")
  @OnOpen
  public void onOpen(final Session session, final EndpointConfig unused) {
    log.debug("Opening websocket session {}", session.getId());
    if (!ksqlEngine.isAcceptingStatements()) {
      log.info("The cluster has been terminated. No new request will be accepted.");
      SessionUtil.closeSilently(
          session,
          CloseCodes.CANNOT_ACCEPT,
          "The cluster has been terminated. No new request will be accepted."
      );
      return;
    }
    try {
      validateVersion(session);

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

      final Statement statement = parseStatement(request);

      HANDLER_MAP
          .getOrDefault(statement.getClass(), WSQueryEndpoint::handleUnsupportedStatement)
          .handle(this, new SessionAndRequest(session, request), statement);
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
    log.debug(
        "Closing websocket session {} ({}): {}",
        session.getId(),
        closeReason.getCloseCode(),
        closeReason.getReasonPhrase()
    );
  }

  @OnError
  public void onError(final Session session, final Throwable t) {
    log.error("websocket error in session {}", session.getId(), t);
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

  private Statement parseStatement(final KsqlRequest request) {
    try {
      return statementParser.parseSingleStatement(request.getKsql());
    } catch (final Exception e) {
      throw new IllegalArgumentException("Error parsing query: " + e.getMessage(), e);
    }
  }

  private void handleQuery(final SessionAndRequest info, final Query ignored) {
    final Map<String, Object> clientLocalProperties = info.request.getStreamsProperties();

    final WebSocketSubscriber<StreamedRow> streamSubscriber =
        new WebSocketSubscriber<>(info.session, mapper);
    this.subscriber = streamSubscriber;

    queryPublisher.start(ksqlConfig, ksqlEngine, exec, info.request.getKsql(),
        clientLocalProperties, streamSubscriber);
  }

  private void handlePrintTopic(final SessionAndRequest info, final PrintTopic printTopic) {
    final String topicName = printTopic.getTopic().toString();

    if (!serviceContext.getTopicClient().isTopicExists(topicName)) {
      throw new IllegalArgumentException("topic does not exist: " + topicName);
    }

    final WebSocketSubscriber<String> topicSubscriber =
        new WebSocketSubscriber<>(info.session, mapper);
    this.subscriber = topicSubscriber;

    topicPublisher.start(exec, serviceContext.getSchemaRegistryClient(),
        ksqlConfig.getKsqlStreamConfigProps(), topicName, printTopic.getFromBeginning(),
        topicSubscriber
    );
  }

  @SuppressWarnings("unused")
  private void handleUnsupportedStatement(
      final SessionAndRequest ignored,
      final Statement statement
  ) {
    throw new IllegalArgumentException(String.format(
        "Statement type `%s' not supported for this resource",
        statement.getClass().getName()
    ));
  }

  private static void startQueryPublisher(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final ListeningScheduledExecutorService exec,
      final String queryString,
      final Map<String, Object> clientLocalProperties,
      final WebSocketSubscriber<StreamedRow> streamSubscriber
  ) {
    new StreamPublisher(ksqlConfig, ksqlEngine, exec, queryString, clientLocalProperties)
        .subscribe(streamSubscriber);
  }

  private static void startPrintPublisher(
      final ListeningScheduledExecutorService exec,
      final SchemaRegistryClient schemaRegistryClient,
      final Map<String, Object> ksqlStreamConfigProps,
      final String topicName,
      final boolean fromBeginning,
      final WebSocketSubscriber<String> topicSubscriber
  ) {
    new PrintPublisher(exec, schemaRegistryClient, ksqlStreamConfigProps, topicName,fromBeginning)
        .subscribe(topicSubscriber);
  }

  interface QueryPublisher {
    void start(
        KsqlConfig ksqlConfig,
        KsqlEngine ksqlEngine,
        ListeningScheduledExecutorService exec,
        String queryString,
        Map<String, Object> clientLocalProperties,
        WebSocketSubscriber<StreamedRow> subscriber);

  }

  interface PrintTopicPublisher {

    void start(
        ListeningScheduledExecutorService exec,
        SchemaRegistryClient schemaRegistryClient,
        Map<String, Object> consumerProperties,
        String topicName,
        boolean fromBeginning,
        WebSocketSubscriber<String> subscriber);
  }

  private static final class SessionAndRequest {

    private final Session session;
    private final KsqlRequest request;

    private SessionAndRequest(final Session session, final KsqlRequest request) {
      this.session = session;
      this.request = request;
    }
  }
}
