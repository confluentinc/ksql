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

import static io.confluent.ksql.api.server.ServerUtils.deserialiseObject;

import io.confluent.ksql.api.server.protocol.ErrorResponse;
import io.confluent.ksql.api.server.protocol.InsertsStreamArgs;
import io.confluent.ksql.api.server.protocol.PojoCodec;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.InsertsSubscriber;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.web.RoutingContext;
import java.util.Objects;

/**
 * This class handles the parsing of the request body for a stream of inserts. The user can send a
 * stream of inserts to the server by opening an HTTP2 request to the server at the appropriate uri
 * and POSTing a request. The request must contain an initial JSON object (encoded as UTF-8 text)
 * which contains the arguments for the request (e.g. the target to insert into, and whether acks
 * are wanted) This must be followed by a new line, and then followed by zero or more JSON objects
 * (also encoded as UTF-8 text) each representing a row to insert. The last JSON object must be
 * followed by a new-line.
 */
public class InsertsBodyHandler {

  private final Context ctx;
  private final Endpoints endpoints;
  private final RoutingContext routingContext;
  private final RecordParser recordParser;
  private boolean hasReadArguments;
  private BufferedPublisher<JsonObject> publisher;
  private long rowsReceived;
  private AcksSubscriber acksSubscriber;

  public InsertsBodyHandler(final Context ctx, final Endpoints endpoints,
      final RoutingContext routingContext) {
    this.ctx = ctx;
    this.endpoints = Objects.requireNonNull(endpoints);
    this.routingContext = Objects.requireNonNull(routingContext);
    this.recordParser = RecordParser.newDelimited("\n", routingContext.request());
  }

  public void handleBodyEnd(final Void v) {
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

  public void handleBodyBuffer(final Buffer buff) {
    if (!hasReadArguments) {
      hasReadArguments = true;
      final InsertsStreamArgs insertsStreamArgs = deserialiseObject(buff, routingContext.response(),
          InsertsStreamArgs.class);
      if (insertsStreamArgs == null) {
        return;
      }

      acksSubscriber =
          insertsStreamArgs.requiresAcks ? new AcksSubscriber(ctx, routingContext.response())
              : null;
      final InsertsSubscriber insertsSubscriber = endpoints
          .createInsertsSubscriber(insertsStreamArgs.target, insertsStreamArgs.properties,
              acksSubscriber);
      publisher = new BufferedPublisher<>(ctx);

      // This forces response headers to be written so we know we send a 200 OK
      // This is important if we subsequently find an error in the stream
      routingContext.response().write("");

      publisher.subscribe(insertsSubscriber);

    } else if (publisher != null) {
      final JsonObject row;
      try {
        row = new JsonObject(buff);
      } catch (DecodeException e) {
        final ErrorResponse errorResponse = new ErrorResponse(
            ErrorCodes.ERROR_CODE_MALFORMED_REQUEST,
            "Invalid JSON in inserts stream");
        routingContext.response().write(PojoCodec.serializeObject(errorResponse).appendString("\n"))
            .end();
        acksSubscriber.cancel();
        return;
      }
      final boolean bufferFull = publisher.accept(row);
      if (bufferFull) {
        recordParser.pause();
        publisher.drainHandler(recordParser::resume);
      }
      rowsReceived++;
    }
  }

}
