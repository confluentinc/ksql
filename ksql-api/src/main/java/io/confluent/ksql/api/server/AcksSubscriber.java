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

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * As the stream of inserts is processed by the back-end, it communicates success (or failure) of
 * each insert by sending a stream of acks back in the other direction. This class is the subscriber
 * which subscribes to that stream of acks and sends them back to the client. It's a reactive
 * streams subscriber so implements back pressure.
 */
public class AcksSubscriber implements Subscriber<Void> {

  private static final Logger log = LoggerFactory.getLogger(AcksSubscriber.class);
  private static final int BATCH_SIZE = 4;
  private static final Buffer ACK_RESPONSE_LINE = new JsonObject().put("status", "ok").toBuffer()
      .appendString("\n");

  private final HttpServerResponse response;
  private Subscription subscription;
  private long tokens;
  private Long insertsSent;
  private long acksSent;

  public AcksSubscriber(final HttpServerResponse response) {
    this.response = Objects.requireNonNull(response);
  }

  @Override
  public synchronized void onSubscribe(final Subscription subscription) {
    Objects.requireNonNull(subscription);
    if (this.subscription != null) {
      throw new IllegalStateException("Already subscribed");
    }
    this.subscription = subscription;
    checkRequestTokens();
  }

  @Override
  public synchronized void onNext(final Void vo) {
    if (tokens == 0) {
      throw new IllegalStateException("Unsolicited data");
    }
    response.write(ACK_RESPONSE_LINE);
    acksSent++;
    tokens--;
    if (insertsSent != null && insertsSent == acksSent) {
      close();
    } else if (response.writeQueueFull()) {
      response.drainHandler(v -> checkRequestTokens());
    } else {
      checkRequestTokens();
    }
  }

  synchronized void insertsSent(final long num) {
    this.insertsSent = num;
    if (acksSent == num) {
      close();
    }
  }

  private void close() {
    response.end();
    subscription.cancel();
  }

  private void checkRequestTokens() {
    if (tokens == 0) {
      tokens = BATCH_SIZE;
      subscription.request(BATCH_SIZE);
    }
  }

  @Override
  public synchronized void onError(final Throwable t) {
    log.error("Error in processing inserts", t);
    final JsonObject err = new JsonObject().put("status", "error")
        .put("errorCode", ERROR_CODE_INTERNAL_ERROR)
        .put("message", "Error in processing inserts");
    subscription.cancel();
    response.end(err.toBuffer());
  }

  @Override
  public synchronized void onComplete() {
    response.end();
  }
}