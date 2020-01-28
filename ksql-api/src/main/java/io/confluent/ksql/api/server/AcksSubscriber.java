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

import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_INTERNAL_ERROR;

import io.confluent.ksql.api.server.protocol.ErrorResponse;
import io.confluent.ksql.api.server.protocol.InsertAck;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reactive streams subscriber that subscribes to publishers of acks. As it receive acks it writes
 * them to the HTTP response.
 */
public class AcksSubscriber extends ReactiveSubscriber<JsonObject> {

  private static final Logger log = LoggerFactory.getLogger(AcksSubscriber.class);
  private static final int BATCH_SIZE = 4;
  private static final Buffer OK_INSERT_RESPONSE_LINE = new InsertAck().toBuffer()
      .appendString("\n");
  private static final int REQUEST_BATCH_SIZE = 1000;

  private final HttpServerResponse response;
  private final InsertsStreamResponseWriter insertsStreamResponseWriter;
  private Long insertsSent;
  private long acksSent;
  private boolean drainHandlerSet;

  public AcksSubscriber(final Context context, final HttpServerResponse response,
      final InsertsStreamResponseWriter insertsStreamResponseWriter) {
    super(context);
    this.response = Objects.requireNonNull(response);
    this.insertsStreamResponseWriter = Objects.requireNonNull(insertsStreamResponseWriter);
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    makeRequest(REQUEST_BATCH_SIZE);
  }

  @Override
  public void handleValue(final JsonObject value) {
    checkContext();
    insertsStreamResponseWriter.writeInsertResponse();
    acksSent++;
    if (insertsSent != null && insertsSent == acksSent) {
      close();
    } else if (response.writeQueueFull()) {
      if (!drainHandlerSet) {
        response.drainHandler(v -> {
          drainHandlerSet = false;
          checkMakeRequest();
        });
        drainHandlerSet = true;
      }
    } else {
      checkMakeRequest();
    }
  }

  @Override
  public void handleComplete() {
    insertsStreamResponseWriter.end();
  }

  @Override
  public void handleError(final Throwable t) {
    log.error("Error in processing inserts", t);

    final ErrorResponse errResponse = new ErrorResponse(ERROR_CODE_INTERNAL_ERROR,
        "Error in processing inserts");
    insertsStreamResponseWriter.writeError(errResponse).end();

  }

  private void checkMakeRequest() {
    if (acksSent % REQUEST_BATCH_SIZE == 0) {
      makeRequest(REQUEST_BATCH_SIZE);
    }
  }

  private void close() {
    insertsStreamResponseWriter.end();
    complete();
  }

  void insertsSent(final long num) {
    this.insertsSent = num;
    if (acksSent == num) {
      close();
    }
  }

}
