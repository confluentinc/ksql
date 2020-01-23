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
 * TODO show format with example
 */
public class JsonInsertsStreamResponseWriter implements InsertsStreamResponseWriter {

  private static final Buffer ACK_RESPONSE_LINE = new JsonObject().put("status", "ok").toBuffer();

  protected final HttpServerResponse response;
  private boolean dataWritten;

  public JsonInsertsStreamResponseWriter(final HttpServerResponse response) {
    this.response = response;
  }

  @Override
  public InsertsStreamResponseWriter writeInsertResponse() {
    writeBuffer(ACK_RESPONSE_LINE);
    return this;
  }

  @Override
  public InsertsStreamResponseWriter writeError(final JsonObject error) {
    writeBuffer(error.toBuffer());
    return this;
  }

  @Override
  public void end() {
    if (!dataWritten) {
      response.write("[]").end();
    } else {
      response.write("]").end();
    }
  }

  private void writeBuffer(final Buffer buffer) {
    if (dataWritten) {
      final Buffer buff = Buffer.buffer().appendByte((byte) ',');
      buff.appendBuffer(buffer);
      response.write(buff);
    } else {
      final Buffer buff = Buffer.buffer().appendByte((byte) '[');
      buff.appendBuffer(buffer);
      response.write(buff);
      dataWritten = true;
    }
  }

}
