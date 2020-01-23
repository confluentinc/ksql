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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Show format with example
 */
public class JsonQueryStreamResponseWriter implements QueryStreamResponseWriter {

  private final HttpServerResponse response;

  public JsonQueryStreamResponseWriter(final HttpServerResponse response) {
    this.response = response;
  }

  @Override
  public QueryStreamResponseWriter writeMetadata(final JsonObject metaData) {
    final Buffer buff = Buffer.buffer().appendByte((byte) '[');
    buff.appendBuffer(metaData.toBuffer());
    response.write(buff);
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeRow(final JsonArray row) {
    writeBuffer(row.toBuffer());
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeError(final JsonObject error) {
    writeBuffer(error.toBuffer());
    return this;
  }

  private void writeBuffer(final Buffer buffer) {
    final Buffer buff = Buffer.buffer().appendByte((byte) ',');
    buff.appendBuffer(buffer);
    response.write(buff);
  }

  @Override
  public void end() {
    response.write("]").end();
  }
}
