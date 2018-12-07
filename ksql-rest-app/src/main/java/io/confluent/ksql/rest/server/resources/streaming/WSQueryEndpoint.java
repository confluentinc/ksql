/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

  private final KsqlConfig ksqlConfig;
  private final ObjectMapper mapper;
  private final StatementParser statementParser;
  private final KsqlEngine ksqlEngine;
  private final ListeningScheduledExecutorService exec;
  private final ActivenessRegistrar activenessRegistrar;

  private WebSocketSubscriber subscriber;

  public WSQueryEndpoint(
      final KsqlConfig ksqlConfig,
      final ObjectMapper mapper,
      final StatementParser statementParser,
      final KsqlEngine ksqlEngine,
      final ListeningScheduledExecutorService exec,
      final ActivenessRegistrar activenessRegistrar
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.statementParser = Objects.requireNonNull(statementParser, "statementParser");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.exec = Objects.requireNonNull(exec, "exec");
    this.activenessRegistrar =
        Objects.requireNonNull(activenessRegistrar, "activenessRegistrar");
  }

  @OnOpen
  public void onOpen(final Session session, final EndpointConfig endpointConfig) {
    log.debug("Opening websocket session {}", session.getId());
    final Map<String, List<String>> parameters = session.getRequestParameterMap();
    activenessRegistrar.updateLastRequestTime();

    final List<String> versionParam = parameters.getOrDefault(
        Versions.KSQL_V1_WS_PARAM, Arrays.asList(Versions.KSQL_V1_WS));
    if (versionParam.size() != 1 || !versionParam.get(0).equals(Versions.KSQL_V1_WS)) {
      log.debug("Received invalid api version: {}", String.join(",", versionParam));
      closeSession(
          session,
          new CloseReason(CloseCodes.CANNOT_ACCEPT, "Invalid version in request")
      );
      return;
    }

    final KsqlRequest request;
    final String queryString;
    final Statement statement;
    try {
      final String requestParam = Objects.requireNonNull(
          getLast("request", parameters),
          "missing request parameter"
      );
      request = mapper.readValue(requestParam, KsqlRequest.class);
      queryString = request.getKsql();
      if (queryString.isEmpty()) {
        throw new IllegalArgumentException("\"ksql\" field must be populated");
      }
      statement = statementParser.parseSingleStatement(queryString);
    } catch (final Exception e) {
      log.debug("Unable to parse query", e);
      closeSession(session, new CloseReason(
          CloseCodes.CANNOT_ACCEPT,
          // don't include error message, since reason is limited to 123 bytes
          "Error parsing query"
      ));
      return;
    }

    try {
      if (statement instanceof Query) {
        final Map<String, Object> clientLocalProperties = request.getStreamsProperties();

        final WebSocketSubscriber<StreamedRow> streamSubscriber =
            new WebSocketSubscriber<>(session, mapper);
        this.subscriber = streamSubscriber;

        new StreamPublisher(ksqlConfig, ksqlEngine, exec, queryString, clientLocalProperties)
            .subscribe(streamSubscriber);
      } else if (statement instanceof PrintTopic) {
        final PrintTopic printTopic = (PrintTopic) statement;
        final String topicName = printTopic.getTopic().toString();

        if (!ksqlEngine.getTopicClient().isTopicExists(topicName)) {
          closeSession(session, new CloseReason(
                  CloseCodes.CANNOT_ACCEPT,
                  "topic does not exist"
          ));
          return;
        }
        final WebSocketSubscriber<String> topicSubscriber =
            new WebSocketSubscriber<>(session, mapper);
        this.subscriber = topicSubscriber;

        new PrintPublisher(
            exec,
            ksqlEngine.getSchemaRegistryClient(),
            ksqlConfig.getKsqlStreamConfigProps(),
            topicName,
            printTopic.getFromBeginning()
        ).subscribe(topicSubscriber);
      } else {
        closeSession(session, new CloseReason(
            CloseCodes.CANNOT_ACCEPT, String.format(
            "Statement type `%s' not supported for this resource",
            statement.getClass().getName()
        )));
      }
    } catch (final Exception e) {
      log.error("Error initializing query in session {}", session.getId(), e);
      closeSession(session, new CloseReason(
          CloseCodes.UNEXPECTED_CONDITION,
          "error initializing query"
      ));
    }
  }

  static void closeSession(final Session session, final CloseReason reason) {
    try {
      session.close(reason);
    } catch (final IOException e) {
      log.error("Exception caught closing session {}", session.getId(), e);
    }
  }

  @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
  private static String getLast(final String name, final Map<String, List<String>> parameters) {
    return Iterables.getLast(parameters.get(name), null);
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

}
