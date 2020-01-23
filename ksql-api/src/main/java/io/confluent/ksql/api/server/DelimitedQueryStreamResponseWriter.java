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

import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * TODO show format with example
 */
public class DelimitedQueryStreamResponseWriter implements QueryStreamResponseWriter {

  private final HttpServerResponse response;

  public DelimitedQueryStreamResponseWriter(final HttpServerResponse response) {
    this.response = response;
  }

  @Override
  public QueryStreamResponseWriter writeMetadata(final JsonObject metaData) {
    response.write(metaData.toBuffer().appendString("\n"));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeRow(final JsonArray row) {
    response.write(row.toBuffer().appendString("\n"));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeError(final JsonObject error) {
    response.write(error.toBuffer().appendString("\n"));
    return this;
  }

  @Override
  public void end() {
    response.end();
  }
}
