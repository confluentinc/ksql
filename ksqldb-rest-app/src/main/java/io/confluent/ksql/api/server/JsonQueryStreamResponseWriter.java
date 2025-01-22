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
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.util.Objects;

/**
 * Writes the query response stream in JSON format.
 *
 * <p>The completed response will form a single JSON array.
 *
 * <p>Providing the response as a single valid JSON array can make it easier to parse with some
 * clients. However this should be used with caution with very large responses when not using a
 * streaming JSON parser as the entire response will have to be stored in memory.
 *
 * <p>The first entry in the array is a JSON object representing the metadata of the query.
 * It contains the column names, column types, query ID, and number of rows (in the case of a pull
 * query).
 *
 * <p>Each subsequent entry in the array is a JSON array representing the values of the columns
 * returned by the query.
 *
 * <p>Please consult the API documentation for a full description of the format.
 */
public class JsonQueryStreamResponseWriter implements QueryStreamResponseWriter {

  private final HttpServerResponse response;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public JsonQueryStreamResponseWriter(final HttpServerResponse response) {
    this.response = Objects.requireNonNull(response);
  }

  @Override
  public QueryStreamResponseWriter writeMetadata(final QueryResponseMetadata metaData) {
    final Buffer buff = Buffer.buffer().appendByte((byte) '[');
    buff.appendBuffer(ServerUtils.serializeObject(metaData));
    response.write(buff);
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeRow(final GenericRow row) {
    writeBuffer(ServerUtils.serializeObject(row.values()));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeContinuationToken(
      final PushContinuationToken pushContinuationToken) {
    writeBuffer(ServerUtils.serializeObject(pushContinuationToken));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeError(final KsqlErrorMessage error) {
    writeBuffer(ServerUtils.serializeObject(error));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeConsistencyToken(final ConsistencyToken consistencyToken) {
    writeBuffer(ServerUtils.serializeObject(consistencyToken));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeCompletionMessage(final String completionMessage) {
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeLimitMessage() {
    return this;
  }

  private void writeBuffer(final Buffer buffer) {
    final Buffer buff = Buffer.buffer().appendByte((byte) ',');
    buff.appendBuffer(buffer);
    response.write(buff);
  }

  @Override
  public void end() {
    response.write("]");
    response.end();
  }
}
