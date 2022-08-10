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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.rest.entity.InsertAck;
import io.confluent.ksql.rest.entity.InsertError;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(JsonInsertsStreamResponseWriter.class);

  protected final HttpServerResponse response;
  private final UUID uuid;
  private boolean dataWritten;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public JsonInsertsStreamResponseWriter(final HttpServerResponse response, final UUID uuid) {
    this.response = Objects.requireNonNull(response);
    this.uuid = uuid;
  }

  @Override
  public InsertsStreamResponseWriter writeInsertResponse(final InsertAck insertAck) {
    writeBuffer(ServerUtils.serializeObject(insertAck));
    return this;
  }

  @Override
  public InsertsStreamResponseWriter writeError(final InsertError error) {
    writeBuffer(ServerUtils.serializeObject(error));
    return this;
  }

  @Override
  public void end() {
    LOG.debug("({}) Ending response for insert stream. Data written: {}", uuid, dataWritten);
    if (!dataWritten) {
      response.write("[]");
    } else {
      response.write("]");
    }
    response.end();
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
