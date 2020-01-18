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

import io.confluent.ksql.api.server.protocol.InsertResponse;
import io.confluent.ksql.api.server.protocol.PojoCodec;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
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
  private static final Buffer OK_INSERT_RESPONSE_LINE = PojoCodec
      .serializeObject(new InsertResponse())
      .appendString("\n");
  private static final int REQUEST_BATCH_SIZE = 1000;

  private final HttpServerResponse response;
  private Long insertsSent;
  private long acksSent;
  private boolean drainHandlerSet;
  private Subscription subscription;
  private boolean cancelled;

  public AcksSubscriber(final Context context, final HttpServerResponse response) {
    super(context);
    this.response = response;
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    makeRequest(REQUEST_BATCH_SIZE);
    this.subscription = subscription;
  }

  @Override
  public void handleValue(final JsonObject value) {
    checkContext();
    if (cancelled) {
      return;
    }
    response.write(OK_INSERT_RESPONSE_LINE);
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
    response.end();
  }

  @Override
  public void handleError(final Throwable t) {
    if (cancelled) {
      return;
    }
    log.error("Error in processing inserts", t);
    final InsertResponse errResponse = new InsertResponse(ERROR_CODE_INTERNAL_ERROR,
        "Error in processing inserts");
    subscription.cancel();
    response.end(PojoCodec.serializeObject(errResponse));
  }

  public void cancel() {
    checkContext();
    cancelled = true;
    if (subscription != null) {
      subscription.cancel();
    }
  }

  private void checkMakeRequest() {
    if (acksSent % REQUEST_BATCH_SIZE == 0) {
      makeRequest(REQUEST_BATCH_SIZE);
    }
  }

  private void close() {
    response.end();
    complete();
  }

  void insertsSent(final long num) {
    this.insertsSent = num;
    if (acksSent == num) {
      close();
    }
  }

}
