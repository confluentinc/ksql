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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
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
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.protobuf.ProtobufNoSRSerdeFactory;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlHostInfo;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public class JsonStreamedRowResponseWriter implements QueryStreamResponseWriter {
  // Make sure this is not too large or else we will overwhelm the response write buffer which
  // appears to corrupt the output.
  private static final int FLUSH_SIZE_BYTES = 5 * 1024;
  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();
  static final long MAX_FLUSH_MS = 200;

  private final HttpServerResponse response;
  private final Optional<TombstoneFactory> tombstoneFactory;
  private final Optional<String> completionMessage;
  private final Optional<String> limitMessage;
  private final Clock clock;
  // If set, we buffer not only the output response, but also the last row, so that we don't have to
  // output a final message.
  private final boolean bufferOutput;
  private final Context context;
  private final RowFormat rowFormat;
  private final WriterState writerState;
  private StreamedRow lastRow;
  private long timerId = -1;


  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public JsonStreamedRowResponseWriter(
      final HttpServerResponse response,
      final QueryPublisher queryPublisher,
      final Optional<String> completionMessage,
      final Optional<String> limitMessage,
      final Clock clock,
      final boolean bufferOutput,
      final Context context,
      final RowFormat rowFormat
  ) {
    this.response = response;
    this.tombstoneFactory = queryPublisher.getResultType().map(
        resultType -> TombstoneFactory.create(queryPublisher.geLogicalSchema(), resultType));
    this.completionMessage = completionMessage;
    this.limitMessage = limitMessage;
    this.writerState = new WriterState(clock);
    this.clock = clock;
    this.bufferOutput = bufferOutput;
    this.context = context;
    Preconditions.checkState(bufferOutput || limitMessage.isPresent()
            || completionMessage.isPresent(),
        "If buffering isn't used, a limit/completion message must be set");
    this.rowFormat = rowFormat;
  }

  @Override
  public QueryStreamResponseWriter writeMetadata(final QueryResponseMetadata metaData) {
    final StreamedRow streamedRow = rowFormat.metadataRow(metaData);
    final Buffer buff = Buffer.buffer().appendByte((byte) '[');
    if (bufferOutput) {
      writeBuffer(buff, true);
      maybeCacheRowAndWriteLast(streamedRow);
    } else {
      // Avoid writing separate chunks for the '[' if we're just going to write it immediately
      // anyway.
      buff.appendBuffer(serializeObject(streamedRow));
      writeBuffer(buff, false);
    }
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
    } else {
      streamedRow = rowFormat.dataRow(keyValueMetadata);
    }
    maybeCacheRowAndWriteLast(streamedRow);
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeContinuationToken(
      final PushContinuationToken pushContinuationToken) {
    final StreamedRow streamedRow = StreamedRow.continuationToken(pushContinuationToken);
    maybeCacheRowAndWriteLast(streamedRow);
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeError(final KsqlErrorMessage error) {
    final StreamedRow streamedRow = StreamedRow.error(error);
    maybeCacheRowAndWriteLast(streamedRow);
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeConsistencyToken(final ConsistencyToken consistencyToken) {
    final StreamedRow streamedRow = StreamedRow.consistencyToken(consistencyToken);
    maybeCacheRowAndWriteLast(streamedRow);
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeCompletionMessage() {
    if (completionMessage.isPresent()) {
      writeLastRow(false);
      final StreamedRow streamedRow = StreamedRow.finalMessage(completionMessage.get());
      writeBuffer(serializeObject(streamedRow), true);
    }
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeLimitMessage() {
    if (limitMessage.isPresent()) {
      writeLastRow(false);
      final StreamedRow streamedRow = StreamedRow.finalMessage(limitMessage.get());
      writeBuffer(serializeObject(streamedRow), true);
    }
    return this;
  }

  @Override
  public void end() {
    cancelTimer();
    writeLastRow(true);
    writeBuffer(Buffer.buffer("]"), true);
    if (writerState.length() > 0) {
      response.write(writerState.getStringToFlush());
    }
    response.end();
  }

  public enum RowFormat {
    PROTOBUF {
      private transient ConnectSchema connectSchema;
      private transient KsqlConnectSerializer<Struct> serializer;
      @Override
      public StreamedRow metadataRow(final QueryResponseMetadata metaData) {
        final LogicalSchema schema = metaData.schema;
        final String queryId = metaData.queryId;

        connectSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());
        serializer = new ProtobufNoSRSerdeFactory(ImmutableMap.of())
                .createSerializer(connectSchema, Struct.class, false);
        return StreamedRow.headerProtobuf(
                new QueryId(queryId), schema, logicalToProtoSchema(schema));
      }

      @Override
      public StreamedRow dataRow(final KeyValueMetadata<List<?>, GenericRow> keyValueMetadata) {
        final KeyValue<List<?>, GenericRow> keyValue = keyValueMetadata.getKeyValue();
        final Struct ksqlRecord = new Struct(connectSchema);
        int i = 0;
        for (Field field : connectSchema.fields()) {
          ksqlRecord.put(
                  field,
                  keyValue.value().get(i));
          i += 1;
        }
        final byte[] protoMessage = serializer.serialize("", ksqlRecord);
        return StreamedRow.pullRowProtobuf(protoMessage);
      }
    },
    JSON {
      @Override
      public StreamedRow metadataRow(final QueryResponseMetadata metaData) {
        return StreamedRow.header(new QueryId(metaData.queryId), metaData.schema);
      }

      @Override
      public StreamedRow dataRow(final KeyValueMetadata<List<?>, GenericRow> keyValueMetadata) {
        final KeyValue<List<?>, GenericRow> keyValue = keyValueMetadata.getKeyValue();
        if (keyValueMetadata.getRowMetadata().isPresent()
                && keyValueMetadata.getRowMetadata().get().getSourceNode().isPresent()) {
          return StreamedRow.pullRow(keyValue.value(),
                  toKsqlHostInfoEntity(keyValueMetadata.getRowMetadata().get().getSourceNode()));
        } else {
          // Technically, this codepath is for both push and pull, but where there's no additional
          // metadata, as there sometimes is with a pull query.
          return StreamedRow.pushRow(keyValue.value());
        }
      }
    };

    public abstract StreamedRow metadataRow(QueryResponseMetadata metaData);

    public abstract StreamedRow dataRow(KeyValueMetadata<List<?>, GenericRow> keyValueMetadata);
  }

  @VisibleForTesting
  static String logicalToProtoSchema(final LogicalSchema schema) {
    final ConnectSchema connectSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());

    final ProtobufSchema protobufSchema = new ProtobufData(
            new ProtobufDataConfig(ImmutableMap.of())).fromConnectSchema(connectSchema);
    return protobufSchema.canonicalString();
  }

  // This does the writing of the rows and possibly caches the current row, writing the last cached
  // value.
  private void maybeCacheRowAndWriteLast(final StreamedRow streamedRow) {
    // If buffering is enabled, we buffer a row and don't require a completion or limit message. If
    // it isn't enabled, we require them in order to make sure we write proper json and don't
    // leave a trailing comma before the end.
    final StreamedRow outputRow;
    if (bufferOutput) {
      outputRow = this.lastRow;
      this.lastRow = streamedRow;
    } else {
      outputRow = streamedRow;
    }
    if (outputRow != null) {
      writeBuffer(serializeObject(outputRow), false);
      maybeFlushBuffer();
    }
  }

  private void writeLastRow(final boolean isLast) {
    final StreamedRow lastRow = this.lastRow;
    this.lastRow = null;
    if (lastRow != null) {
      writeBuffer(serializeObject(lastRow), isLast);
    }
  }

  private void writeBuffer(final Buffer buffer, final boolean isLastOrFirst) {
    if (bufferOutput) {
      writerState.append(buffer.toString(StandardCharsets.UTF_8));
      if (!isLastOrFirst) {
        writerState.append(",\n");
      }
    } else {
      final Buffer buff = Buffer.buffer();
      buff.appendBuffer(buffer);
      if (!isLastOrFirst) {
        buff.appendString(",\n");
      }
      response.write(buff);
    }
  }

  private void maybeFlushBuffer() {
    if (writerState.length() > 0) {
      if (writerState.length() >= FLUSH_SIZE_BYTES
          || clock.millis() - writerState.getLastFlushMs() >= MAX_FLUSH_MS
      ) {
        cancelTimer();
        response.write(writerState.getStringToFlush());
      } else {
        maybeScheduleFlush();
      }
    }
  }

  private void maybeScheduleFlush() {
    if (timerId >= 0) {
      return;
    }
    final long sinceLastFlushMs = clock.millis() - writerState.getLastFlushMs();
    final long waitTimeMs = Math.min(Math.max(0, MAX_FLUSH_MS - sinceLastFlushMs), MAX_FLUSH_MS);
    this.timerId = context.owner().setTimer(waitTimeMs, timerId -> {
      this.timerId = -1;
      maybeFlushBuffer();
    });
  }

  private void cancelTimer() {
    if (timerId >= 0) {
      context.owner().cancelTimer(timerId);
      timerId = - 1;
    }
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

  private static class WriterState {
    private final Clock clock;
    // The buffer of JSON that we're always flushing as we hit either time or size thresholds.
    private StringBuilder sb = new StringBuilder();
    // Last flush timestamp in millis
    private long lastFlushMs = 0;

    WriterState(final Clock clock) {
      this.clock = clock;
      this.lastFlushMs = clock.millis();
    }

    public WriterState append(final String str) {
      sb.append(str);
      return this;
    }

    public int length() {
      return sb.length();
    }

    public long getLastFlushMs() {
      return lastFlushMs;
    }

    public String getStringToFlush() {
      final String str = sb.toString();
      sb = new StringBuilder();
      lastFlushMs = clock.millis();
      return str;
    }
  }
}
