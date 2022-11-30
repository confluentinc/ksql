/*
 * Copyright 2022 Confluent Inc.
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
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.vertx.core.http.HttpServerResponse;
import java.util.Objects;

/**
 * Writes the print response in delimited format using new line.
 *
 * <p>The response comprises a sequence of topic records, separated by newline. The overall
 * response does not form a single JSON object or array. This makes it easier to parse at the client
 * without recourse to a streaming JSON parser.
 *
 * <p>Each entry in the stream is a string with fixed fields.
 *
 * <p>Please consult the API documentation for a full description of the format. The format used
 * here is exactly the same used by the previous Websocket implementation of the print topic
 * functionality.
 */
public class DelimitedPrintResponseWriter implements PrintResponseWriter {

  private final HttpServerResponse response;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public DelimitedPrintResponseWriter(final HttpServerResponse response) {
    this.response = Objects.requireNonNull(response);
  }

  @Override
  public PrintResponseWriter writeRow(
      final String row
  ) {
    response.write(row + "\n");
    return this;
  }

  @Override
  public PrintResponseWriter writeError(final KsqlErrorMessage error) {
    response.write(ServerUtils.serializeObject(error).appendString("\n"));
    return this;
  }

  @Override
  public void end() {
    response.end();
  }
}
