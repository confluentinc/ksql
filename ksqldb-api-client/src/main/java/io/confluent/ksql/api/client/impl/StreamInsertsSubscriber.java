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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.Objects;
import org.reactivestreams.Subscription;

public class StreamInsertsSubscriber extends BaseSubscriber<Object> {

  private static final Logger log = LoggerFactory.getLogger(StreamInsertsSubscriber.class);

  private static final int REQUEST_BATCH_SIZE = 200;

  private final HttpClientRequest httpRequest;
  private int outstandingTokens;
  private boolean drainHandlerSet;

  public StreamInsertsSubscriber(final Context context, final HttpClientRequest httpRequest) {
    super(context);
    this.httpRequest = Objects.requireNonNull(httpRequest);
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    checkRequest();
  }

  @Override
  protected void handleValue(final Object row) {
    httpRequest.writeCustomFrame(
        0, 0,
        Buffer.buffer().appendString(JsonObject.mapFrom(row).toString()).appendString("\n")
    );
    outstandingTokens--;

    if (httpRequest.writeQueueFull()) {
      if (!drainHandlerSet) {
        httpRequest.drainHandler(this::httpRequestReceptive);
        drainHandlerSet = true;
      } else {
        checkRequest();
      }
    }
  }

  @Override
  protected void handleComplete() {
    httpRequest.end();
  }

  @Override
  protected void handleError(final Throwable t) {
    log.error("Received error from streamInserts() publisher. Ending connection.", t);
    httpRequest.end();
  }

  private void checkRequest() {
    if (outstandingTokens == 0) {
      outstandingTokens = REQUEST_BATCH_SIZE;
      makeRequest(REQUEST_BATCH_SIZE);
    }
  }

  private void httpRequestReceptive(final Void v) {
    drainHandlerSet = false;
    checkRequest();
  }
}
