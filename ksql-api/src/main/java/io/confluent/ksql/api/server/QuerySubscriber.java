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
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import java.util.Objects;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a reactive streams subscriber which receives a stream of results from a publisher which
 * is implemented by the back-end. The results are then written to the HTTP2 response.
 */
public class QuerySubscriber implements Subscriber<JsonArray> {

  private static final Logger log = LoggerFactory.getLogger(QuerySubscriber.class);

  private final HttpServerResponse response;
  private final QueryStreamResponseWriter queryStreamResponseWriter;
  private Subscription subscription;
  private long tokens;

  private static final int BATCH_SIZE = 4;

  public QuerySubscriber(final HttpServerResponse response,
      final QueryStreamResponseWriter queryStreamResponseWriter) {
    this.response = Objects.requireNonNull(response);
    this.queryStreamResponseWriter = Objects.requireNonNull(queryStreamResponseWriter);
  }

  @Override
  public synchronized void onSubscribe(final Subscription subscription) {
    if (this.subscription != null) {
      throw new IllegalStateException("Already subscribed");
    }
    this.subscription = Objects.requireNonNull(subscription);
    checkRequestTokens();
  }

  @Override
  public synchronized void onNext(final JsonArray row) {
    if (tokens == 0) {
      throw new IllegalStateException("Unsolicited data");
    }
    queryStreamResponseWriter.writeRow(row);
    tokens--;
    if (response.writeQueueFull()) {
      response.drainHandler(v -> {
        checkRequestTokens();
      });
    } else {
      checkRequestTokens();
    }
  }

  private void checkRequestTokens() {
    if (tokens == 0) {
      tokens = BATCH_SIZE;
      subscription.request(BATCH_SIZE);
    }
  }

  @Override
  public synchronized void onError(final Throwable t) {
    log.error("Error in processing query", t);
    final ErrorResponse errorResponse = new ErrorResponse(ERROR_CODE_INTERNAL_ERROR,
        "Error in processing query");
    queryStreamResponseWriter.writeError(errorResponse).end();
  }

  @Override
  public synchronized void onComplete() {
    queryStreamResponseWriter.end();
  }

  public synchronized void close() {
    if (subscription != null) {
      subscription.cancel();
    }
  }
}