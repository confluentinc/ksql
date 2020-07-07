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

import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import java.util.concurrent.CompletableFuture;

public class InsertIntoResponseHandler extends ResponseHandler<CompletableFuture<Void>> {

  private int numAcks;

  InsertIntoResponseHandler(
      final Context context, final RecordParser recordParser, final CompletableFuture<Void> cf) {
    super(context, recordParser, cf);
  }

  @Override
  protected void doHandleBodyBuffer(final Buffer buff) {
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

  @Override
  protected void doHandleException(final Throwable t) {
    if (!cf.isDone()) {
      cf.completeExceptionally(t);
    }
  }

  @Override
  protected void doHandleBodyEnd() {
    if (numAcks != 1) {
      throw new IllegalStateException(
          "Received unexpected number of acks from /inserts-stream. "
              + "Expected: 1. Got: " + numAcks);
    }

    if (!cf.isDone()) {
      cf.complete(null);
    }
  }
}
