/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.server;

import static io.confluent.ksql.api.server.QueryStreamHandler.DELIMITED_CONTENT_TYPE;
import static io.confluent.ksql.api.server.ServerUtils.checkHttp2;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_REQUEST;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.auth.DefaultApiSecurityContext;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.entity.InsertError;
import io.confluent.ksql.rest.entity.InsertsStreamArgs;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.web.RoutingContext;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles the parsing of the request body for a stream of inserts. The user can send a
 * stream of inserts to the server by opening an HTTP2 request to the server at the appropriate uri
 * and POSTing a request. The request must contain an initial JSON object (encoded as UTF-8 text)
 * which contains the arguments for the request (e.g. the target to insert into, and whether acks
 * are wanted) This must be followed by a new line, and then followed by zero or more JSON objects
 * (also encoded as UTF-8 text) each representing a row to insert. The last JSON object must be
 * followed by a new-line.
 */
public class InsertsStreamHandler implements Handler<RoutingContext> {

  private static final Logger LOG = LoggerFactory.getLogger(InsertsStreamHandler.class);

  private final Context ctx;
  private final Endpoints endpoints;
  private final Server server;
  private final WorkerExecutor workerExecutor;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public InsertsStreamHandler(final Context ctx, final Endpoints endpoints,
      final Server server) {
    this.ctx = Objects.requireNonNull(ctx);
    this.endpoints = Objects.requireNonNull(endpoints);
    this.server = Objects.requireNonNull(server);
    this.workerExecutor = Objects.requireNonNull(server.getWorkerExecutor());
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    if (!checkHttp2(routingContext)) {
      return;
    }

    // The record parser takes in potentially fragmented buffers from the request and spits
    // out the chunks delimited by newline
    final RecordParser recordParser = RecordParser.newDelimited("\n", routingContext.request());
    final RequestHandler requestHandler = new RequestHandler(routingContext, recordParser);
    recordParser.handler(requestHandler::handleBodyBuffer);
    recordParser.endHandler(requestHandler::handleBodyEnd);
  }

  private class RequestHandler {

    private final RoutingContext routingContext;
    private final RecordParser recordParser;
    private final InsertsStreamResponseWriter insertsStreamResponseWriter;
    private final UUID uuid; // used to correlate logs
    private boolean hasReadArguments;
    private BufferedPublisher<JsonObject> publisher;
    private long rowsReceived;
    private AcksSubscriber acksSubscriber;
    private boolean paused;
    private boolean responseEnded;
    private long sendSequence;
    private InsertsStreamSubscriber insertsSubscriber;

    RequestHandler(final RoutingContext routingContext,
        final RecordParser recordParser) {
      this.routingContext = Objects.requireNonNull(routingContext);
      this.recordParser = Objects.requireNonNull(recordParser);
      this.uuid = UUID.randomUUID();
      final String contentType = routingContext.getAcceptableContentType();
      if (DELIMITED_CONTENT_TYPE.equals(contentType) || contentType == null) {
        // Default
        insertsStreamResponseWriter =
            new DelimitedInsertsStreamResponseWriter(routingContext.response(), uuid);
      } else {
        insertsStreamResponseWriter = new JsonInsertsStreamResponseWriter(
            routingContext.response(), uuid);
      }
    }

    private void handleBodyBuffer(final Buffer buff) {

      if (responseEnded) {
        // Ignore further buffers from request if response has been written (most probably due
        // to error)
        return;
      }
      if (!hasReadArguments) {
        handleArgs(buff);
      } else if (publisher != null) {
        handleRow(buff);
      }
    }

    private void handleArgs(final Buffer buff) {
      hasReadArguments = true;
      final Optional<InsertsStreamArgs> insertsStreamArgs = ServerUtils.deserialiseObject(buff,
          routingContext,
          InsertsStreamArgs.class);
      if (!insertsStreamArgs.isPresent()) {
        return;
      }

      LOG.debug("({}) Processed insert stream args: {}", uuid, insertsStreamArgs.get());

      routingContext.response().endHandler(v -> handleResponseEnd());

      acksSubscriber = new AcksSubscriber(ctx, routingContext.response(),
          insertsStreamResponseWriter);

      recordParser.pause();

      endpoints.createInsertsSubscriber(insertsStreamArgs.get().target,
          insertsStreamArgs.get().properties, acksSubscriber, ctx, workerExecutor,
          DefaultApiSecurityContext.create(routingContext, server))
          .thenAccept(insertsSubscriber -> {
            publisher = new BufferedPublisher<>(ctx);

            // This forces response headers to be written so we know we send a 200 OK
            // This is important if we subsequently find an error in the stream
            LOG.debug(
                "({}) Acknowledging insert stream in subscriber after creating publisher.", uuid);
            routingContext.response().write("");

            publisher.subscribe(insertsSubscriber);

            recordParser.resume();

            this.insertsSubscriber = insertsSubscriber;
          })
          .exceptionally(t ->
              ServerUtils.handleEndpointException(t, routingContext, "Failed to execute inserts"));
    }

    private void handleRow(final Buffer buff) {
      final long seq = sendSequence++;
      final JsonObject row;
      try {
        row = new JsonObject(buff);
        LOG.debug("({}) Handling insert stream row: {}", uuid, row);
      } catch (DecodeException e) {
        final InsertError errorResponse = new InsertError(
            seq,
            ERROR_CODE_BAD_REQUEST,
            "Invalid JSON in inserts stream");
        LOG.warn("({}) Failed to process row at sequence {} ({})",
            uuid, sendSequence, buff.toString(), e);
        insertsStreamResponseWriter.writeError(errorResponse).end();
        acksSubscriber.cancel();
        return;
      }

      final boolean bufferFull = publisher.accept(row);
      if (bufferFull && !paused) {
        LOG.debug("({}) Buffer is full after processing {} records. Pausing the parser",
            uuid, sendSequence);
        recordParser.pause();
        publisher.drainHandler(this::publisherReceptive);
        paused = true;
      }
      rowsReceived++;
    }

    private void publisherReceptive() {
      LOG.debug("({}) Resuming record parser after draining publisher.", uuid);
      paused = false;
      recordParser.resume();
    }

    private void handleBodyEnd(final Void v) {
      LOG.debug("({}) Completed reading the request, ending the response. "
              + "Completing Publisher: {}, Closing Publisher: {}",
          uuid,
          publisher != null,
          acksSubscriber == null);

      if (publisher != null) {
        publisher.complete();
        if (acksSubscriber == null) {
          routingContext.response().end();
        } else {
          // We close the response after the stream of acks has been sent
          acksSubscriber.insertsSent(rowsReceived);
        }
      }
    }

    private void handleResponseEnd() {
      responseEnded = true;
      if (insertsSubscriber != null) {
        insertsSubscriber.close();
      }
    }

  }

}
