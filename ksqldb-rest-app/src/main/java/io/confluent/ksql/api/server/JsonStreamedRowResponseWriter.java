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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.streaming.TombstoneFactory;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlHostInfo;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.util.List;
import java.util.Optional;

public class JsonStreamedRowResponseWriter implements QueryStreamResponseWriter {

  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  private final HttpServerResponse response;
  private final Optional<TombstoneFactory> tombstoneFactory;


  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public JsonStreamedRowResponseWriter(
      final HttpServerResponse response,
      final QueryPublisher queryPublisher
  ) {
    this.response = response;
    this.tombstoneFactory = queryPublisher.getResultType().map(
        resultType -> TombstoneFactory.create(queryPublisher.geLogicalSchema(), resultType));
  }

  @Override
  public QueryStreamResponseWriter writeMetadata(final QueryResponseMetadata metaData) {
    final StreamedRow streamedRow
        = StreamedRow.header(new QueryId(metaData.queryId), metaData.schema);
    final Buffer buff = Buffer.buffer().appendByte((byte) '[');
    buff.appendBuffer(serializeObject(streamedRow));
    buff.appendString(",\n");
    response.write(buff);
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeRow(
      final KeyValueMetadata<List<?>, GenericRow> keyValueMetadata
  ) {
    final KeyValue<List<?>, GenericRow> keyValue = keyValueMetadata.getKeyValue();
    final StreamedRow streamedRow;
    if (keyValue.value() == null) {
      Preconditions.checkState(tombstoneFactory.isPresent(),
          "Should only have null values for query types that support them");
      streamedRow = StreamedRow.tombstone(tombstoneFactory.get().createRow(keyValue));
    } else if (keyValueMetadata.getRowMetadata().isPresent()
        && keyValueMetadata.getRowMetadata().get().getSourceNode().isPresent()) {
      streamedRow = StreamedRow.pullRow(keyValue.value(),
          toKsqlHostInfoEntity(keyValueMetadata.getRowMetadata().get().getSourceNode()));
    } else {
      // Technically, this codepath is for both push and pull, but where there's no additional
      // metadata, as there sometimes is with a pull query.
      streamedRow = StreamedRow.pushRow(keyValue.value());
    }
    writeBuffer(serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeContinuationToken(
      final PushContinuationToken pushContinuationToken) {
    final StreamedRow streamedRow = StreamedRow.continuationToken(pushContinuationToken);
    writeBuffer(serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeError(final KsqlErrorMessage error) {
    final StreamedRow streamedRow = StreamedRow.error(error);
    writeBuffer(serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeConsistencyToken(final ConsistencyToken consistencyToken) {
    final StreamedRow streamedRow = StreamedRow.consistencyToken(consistencyToken);
    writeBuffer(serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeCompletionMessage(final String completionMessage) {
    final StreamedRow streamedRow = StreamedRow.finalMessage(completionMessage);
    writeBuffer(serializeObject(streamedRow), true);
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeLimitMessage() {
    final StreamedRow streamedRow = StreamedRow.finalMessage("Limit Reached");
    writeBuffer(serializeObject(streamedRow), true);
    return this;
  }

  @Override
  public void end() {
    response.write("]").end();
  }

  private void writeBuffer(final Buffer buffer) {
    writeBuffer(buffer, false);
  }

  private void writeBuffer(final Buffer buffer, final boolean isLast) {
    final Buffer buff = Buffer.buffer();
    buff.appendBuffer(buffer);
    if (!isLast) {
      buff.appendString(",\n");
    }
    response.write(buff);
  }

  public static <T> Buffer serializeObject(final T t) {
    try {
      final byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(t);
      return Buffer.buffer(bytes);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize buffer", e);
    }
  }

  /**
   * Converts the KsqlHostInfo to KsqlHostInfoEntity
   */
  private static Optional<KsqlHostInfoEntity> toKsqlHostInfoEntity(
      final Optional<KsqlHostInfo> ksqlNode
  ) {
    return ksqlNode.map(
        node -> new KsqlHostInfoEntity(node.host(), node.port()));
  }
}
