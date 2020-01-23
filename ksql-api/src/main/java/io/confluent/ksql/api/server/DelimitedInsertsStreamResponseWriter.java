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

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;

/**
 * TODO describe format with example
 */
public class DelimitedInsertsStreamResponseWriter implements InsertsStreamResponseWriter {

  private static final Buffer ACK_RESPONSE_LINE = new JsonObject().put("status", "ok").toBuffer()
      .appendString("\n");

  private final HttpServerResponse response;

  public DelimitedInsertsStreamResponseWriter(final HttpServerResponse response) {
    this.response = response;
  }

  @Override
  public InsertsStreamResponseWriter writeInsertResponse() {
    response.write(ACK_RESPONSE_LINE);
    return this;
  }

  @Override
  public InsertsStreamResponseWriter writeError(final JsonObject error) {
    response.write(error.toBuffer().appendString("\n"));
    return this;
  }

  @Override
  public void end() {
    response.end();
  }
}
