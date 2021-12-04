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
  public QueryStreamResponseWriter writeMetadata(QueryResponseMetadata metaData) {
    final StreamedRow streamedRow
        = StreamedRow.header(new QueryId(metaData.queryId), metaData.schema);
    final Buffer buff = Buffer.buffer().appendByte((byte) '[');
    buff.appendBuffer(ServerUtils.serializeObject(streamedRow));
    buff.appendString("\n");
    response.write(buff);
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeRow(GenericRow row) {
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
  public QueryStreamResponseWriter writeError(KsqlErrorMessage error) {
    final StreamedRow streamedRow = StreamedRow.error(error);
    writeBuffer(ServerUtils.serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeConsistencyToken(ConsistencyToken consistencyToken) {
    final StreamedRow streamedRow = StreamedRow.consistencyToken(consistencyToken);
    writeBuffer(ServerUtils.serializeObject(streamedRow));
    return this;
  }

  @Override
  public void end() {
    response.write("]").end();
  }

  private void writeBuffer(final Buffer buffer) {
    final Buffer buff = Buffer.buffer().appendByte((byte) ',');
    buff.appendBuffer(buffer);
    buff.appendString("\n");
    response.write(buff);
  }
}
