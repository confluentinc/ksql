/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;

public class JsonStreamedRowResponseWriter implements QueryStreamResponseWriter {

  private final HttpServerResponse response;

  public JsonStreamedRowResponseWriter(final HttpServerResponse response) {
    this.response = response;
  }

  @Override
  public QueryStreamResponseWriter writeMetadata(final QueryResponseMetadata metaData) {
    final StreamedRow streamedRow
        = StreamedRow.header(new QueryId(metaData.queryId), metaData.schema);
    final Buffer buff = Buffer.buffer().appendByte((byte) '[');
    buff.appendBuffer(ServerUtils.serializeObject(streamedRow));
    buff.appendString(",\n");
    response.write(buff);
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeRow(final GenericRow row) {
    final StreamedRow streamedRow = StreamedRow.pushRow(row);
    writeBuffer(ServerUtils.serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeContinuationToken(
      final PushContinuationToken pushContinuationToken) {
    final StreamedRow streamedRow = StreamedRow.continuationToken(pushContinuationToken);
    writeBuffer(ServerUtils.serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeError(final KsqlErrorMessage error) {
    final StreamedRow streamedRow = StreamedRow.error(error);
    writeBuffer(ServerUtils.serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeConsistencyToken(final ConsistencyToken consistencyToken) {
    final StreamedRow streamedRow = StreamedRow.consistencyToken(consistencyToken);
    writeBuffer(ServerUtils.serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeCompletionMessage(final String completionMessage) {
    final StreamedRow streamedRow = StreamedRow.finalMessage(completionMessage);
    writeBuffer(ServerUtils.serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeLimitMessage() {
    final StreamedRow streamedRow = StreamedRow.finalMessage("Limit Reached");
    writeBuffer(ServerUtils.serializeObject(streamedRow));
    return this;
  }

  @Override
  public void end() {
    response.write("]").end();
  }

  private void writeBuffer(final Buffer buffer) {
    final Buffer buff = Buffer.buffer();
    buff.appendBuffer(buffer);
    buff.appendString(",\n");
    response.write(buff);
  }
}
