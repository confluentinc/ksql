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

import io.confluent.ksql.api.client.KsqlClientException;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class InsertsResponseHandler {

  private final Context context;
  private final CompletableFuture<Void> cf;
  private int numAcks;

  InsertsResponseHandler(final Context context, final CompletableFuture<Void> cf) {
    this.context = Objects.requireNonNull(context);
    this.cf = Objects.requireNonNull(cf);
  }

  public void handleBodyBuffer(final Buffer buff) {
    checkContext();

    final JsonObject jsonObject = new JsonObject(buff);
    final String status = jsonObject.getString("status");
    if ("ok".equals(status)) {
      numAcks++;
    } else if ("error".equals(status)) {
      cf.completeExceptionally(new KsqlClientException(String.format(
          "Received error from /inserts-stream. Error code: %d. Message: %s",
          jsonObject.getInteger("error_code"),
          jsonObject.getString("message")
      )));
    } else {
      throw new IllegalStateException(
          "Unrecognized status response from /inserts-stream: " + status);
    }
  }

  public void handleException(final Throwable t) {
    checkContext();

    if (!cf.isDone()) {
      cf.completeExceptionally(t);
    }
  }

  public void handleBodyEnd(final Void v) {
    checkContext();

    if (numAcks != 1) {
      throw new IllegalStateException(
          "Received unexpected number of acks from /inserts-stream. "
              + "Expected: 1. Got: " + numAcks);
    }

    if (!cf.isDone()) {
      cf.complete(null);
    }
  }

  private void checkContext() {
    VertxUtils.checkContext(context);
  }
}
