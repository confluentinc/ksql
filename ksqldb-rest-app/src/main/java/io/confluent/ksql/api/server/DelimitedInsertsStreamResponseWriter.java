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
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the inserts response stream in delimited format.
 *
 * <p>Each insert in the incoming stream will have a corresponding entry in the response stream, in
 * the same order as the inserts.
 *
 * <p>Each entry is a JSON object, separated by newline. The overall response does not form a
 * single JSON object or array. This makes it easier to parse at the client without recourse to
 * streaming JSON parsers.
 *
 * <p>Please consult the API documentation for a full description of the format.
 */
public class DelimitedInsertsStreamResponseWriter implements InsertsStreamResponseWriter {

  private static final Logger LOG = LoggerFactory.getLogger(
      DelimitedInsertsStreamResponseWriter.class);
  private static final Buffer ACK_RESPONSE_LINE = new JsonObject().put("status", "ok").toBuffer()
      .appendString("\n");

  private final HttpServerResponse response;
  private final UUID uuid;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public DelimitedInsertsStreamResponseWriter(final HttpServerResponse response, final UUID uuid) {
    this.response = Objects.requireNonNull(response);
    this.uuid = uuid;
  }

  @Override
  public InsertsStreamResponseWriter writeInsertResponse(final InsertAck insertAck) {
    response.write(ServerUtils.serializeObject(insertAck).appendString("\n"));
    return this;
  }

  @Override
  public InsertsStreamResponseWriter writeError(final InsertError error) {
    response.write(ServerUtils.serializeObject(error).appendString("\n"));
    return this;
  }

  @Override
  public void end() {
    LOG.debug("({}) Ending response for insert stream", uuid);
    response.end();
  }
}
