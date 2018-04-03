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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.EndpointConfig;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.util.QueuedQueryMetadata;

@ServerEndpoint(value = "/query")
public class WSQueryEndpoint {
  private static final Logger log = LoggerFactory.getLogger(WSQueryEndpoint.class);
  private static final int WS_STREAMS_POLL_DELAY_MS = 500;

  final ObjectMapper mapper;
  final StatementParser statementParser;
  final KsqlEngine ksqlEngine;
  final ListeningScheduledExecutorService exec;

  public WSQueryEndpoint(
      ObjectMapper mapper,
      StatementParser statementParser,
      KsqlEngine ksqlEngine,
      ListeningScheduledExecutorService exec
  ) {
    this.mapper = mapper;
    this.statementParser = statementParser;
    this.ksqlEngine = ksqlEngine;
    this.exec = exec;
  }

  private volatile QueuedQueryMetadata queryMetadata;
  private volatile ListenableScheduledFuture future;

  @OnOpen
  public void onOpen(Session session, EndpointConfig endpointConfig) {
    log.debug("Opening websocket session {}", session.getId());
    final Map<String, List<String>> parameters = session.getRequestParameterMap();

    final KsqlRequest request;
    final String queryString;
    final Statement statement;
    try {
      String requestParam = Objects.requireNonNull(
          getLast("request", parameters),
          "missing request parameter"
      );
      request = mapper.readValue(requestParam, KsqlRequest.class);
      queryString = Objects.requireNonNull(request.getKsql(), "\"ksql\" field must be given");
      statement = statementParser.parseSingleStatement(queryString);
    } catch (Exception e) {
      log.debug("Unable to parse query", e);
      closeSession(session, new CloseReason(
          CloseCodes.CANNOT_ACCEPT,
          // don't include error message, since reason is limited to 123 bytes
          "Error parsing query"
      ));
      return;
    }

    Map<String, Object> clientLocalProperties =
        Optional.ofNullable(request.getStreamsProperties()).orElse(Collections.emptyMap());

    if (!(statement instanceof Query)) {
      closeSession(session, new CloseReason(
          CloseCodes.CANNOT_ACCEPT, String.format(
          "Statement type `%s' not supported for this resource",
          statement.getClass().getName()
      )));
      return;
    }

    try {
      queryMetadata = (QueuedQueryMetadata) ksqlEngine.buildMultipleQueries(
          queryString,
          clientLocalProperties
      ).get(0);

      try {
        session.getBasicRemote().sendBinary(ByteBuffer.wrap(
            mapper.writeValueAsBytes(
                queryMetadata.getOutputNode().getSchema()
            )
        ));
      } catch (IOException e) {
        log.error("Error sending schema", e);
        closeSession(session, new CloseReason(
            CloseCodes.PROTOCOL_ERROR,
            "Unable to send schema"
        ));
        closeAndRemoveQuery();
        return;
      }

      queryMetadata.getKafkaStreams().setUncaughtExceptionHandler((thread, e) -> {
        log.error("streams exception in session {}", session.getId(), e);
        closeSession(session, new CloseReason(
            CloseCodes.UNEXPECTED_CONDITION,
            "streams exception"
        ));
      });
      log.info("Running query {}", queryMetadata.getQueryApplicationId());
      queryMetadata.getKafkaStreams().start();

      future = exec.scheduleWithFixedDelay(() -> {
        List<KeyValue<String, GenericRow>> rows = Lists.newLinkedList();
        queryMetadata.getRowQueue().drainTo(rows);

        for (KeyValue<String, GenericRow> row : rows) {
          try {
            byte[] buffer = mapper.writeValueAsBytes(new StreamedRow(row.value));
            session.getAsyncRemote().sendBinary(
                ByteBuffer.wrap(buffer), result -> {
                  if (!result.isOK()) {
                    log.warn(
                        "Error sending websocket message for session {}",
                        session.getId(),
                        result.getException()
                    );
                  }
                });
          } catch (JsonProcessingException e) {
            log.warn("Error serializing row in session {}", session.getId(), e);
          }
        }
      }, 0, WS_STREAMS_POLL_DELAY_MS, TimeUnit.MILLISECONDS);

      future.addListener(this::closeAndRemoveQuery, exec);
    } catch (Exception e) {
      log.error("Error initializing query in session {}", session.getId(), e);
      closeSession(session, new CloseReason(
          CloseCodes.UNEXPECTED_CONDITION,
          "error initializing query"
      ));
      closeAndRemoveQuery();
    }
  }

  private void closeAndRemoveQuery() {
    if (queryMetadata != null) {
      log.info("Terminating query {}", queryMetadata.getQueryApplicationId());
      queryMetadata.close();
      ksqlEngine.removeTemporaryQuery(queryMetadata);
    }
  }

  private static void closeSession(Session session, CloseReason reason) {
    try {
      session.close(reason);
    } catch (IOException e) {
      log.error("Exception caught closing session {}", session.getId(), e);
    }
  }

  @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
  private static String getLast(String name, Map<String, List<String>> parameters) {
    return Iterables.getLast(parameters.get(name), null);
  }

  @OnClose
  public void onClose(Session session, CloseReason closeReason) {
    log.debug(
        "Closing websocket session {} ({}): {}",
        session.getId(),
        closeReason.getCloseCode(),
        closeReason.getReasonPhrase()
    );
    if (future != null) {
      future.cancel(true);
    }
  }

  @OnError
  public void onError(Session session, Throwable t) {
    log.error("websocket error in session {}", session.getId(), t);
  }
}
