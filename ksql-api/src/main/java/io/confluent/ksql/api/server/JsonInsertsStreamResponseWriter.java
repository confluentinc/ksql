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

import io.confluent.ksql.api.server.protocol.ErrorResponse;
import io.confluent.ksql.api.server.protocol.InsertAck;
import io.confluent.ksql.api.server.protocol.PojoCodec;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.util.Objects;

/**
 * Writes the inserts response stream in JSON format.
 *
 * <p>The completed response will form a single JSON array, and each insert in the incoming stream
 * will have a corresponding entry in the response stream, in the same order as the inserts.
 *
 * <p>Providing the response as a single valid JSON array can make it easier to parse with some
 * clients. However this should be used with caution with very large responses when not using a
 * streaming JSON parser as the entire response will have to be stored in memory.
 *
 * <p>Please consult the API documentation for a full description of the format.
 */
public class JsonInsertsStreamResponseWriter implements InsertsStreamResponseWriter {

  private static final Buffer ACK_RESPONSE_LINE = new InsertAck().toBuffer();

  protected final HttpServerResponse response;
  private boolean dataWritten;

  public JsonInsertsStreamResponseWriter(final HttpServerResponse response) {
    this.response = Objects.requireNonNull(response);
  }

  @Override
  public InsertsStreamResponseWriter writeInsertResponse() {
    writeBuffer(ACK_RESPONSE_LINE);
    return this;
  }

  @Override
  public InsertsStreamResponseWriter writeError(final ErrorResponse error) {
    writeBuffer(PojoCodec.serializeObject(error));
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
