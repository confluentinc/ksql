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

import static io.confluent.ksql.api.server.ServerUtils.serializeObject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.protobuf.ProtobufNoSRSerdeFactory;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG
      = LoggerFactory.getLogger(JsonQueryStreamResponseWriter.class);

  private final HttpServerResponse response;
  private final String mediaType;
  private ConnectSchema connectSchema;
  private KsqlConnectSerializer<Struct> serializer;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public JsonQueryStreamResponseWriter(final HttpServerResponse response, final String mediaType) {
    this.response = Objects.requireNonNull(response);
    this.mediaType = Objects.requireNonNull(mediaType);
  }

  @Override
  public QueryStreamResponseWriter writeMetadata(final QueryResponseMetadata metaData) {
    final Buffer buff = Buffer.buffer().appendByte((byte) '[');
    if (mediaType.equals(KsqlMediaType.KSQL_V1_PROTOBUF.mediaType())) {
      final LogicalSchema schema = metaData.schema;
      final String queryId = metaData.queryId;

      connectSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());
      serializer = new ProtobufNoSRSerdeFactory(ImmutableMap.of())
              .createSerializer(connectSchema, Struct.class, false);

      final StreamedRow header = StreamedRow.headerProtobuf(
              new QueryId(queryId), logicalToProtoSchema(schema));

      buff.appendBuffer(serializeObject(header));
    } else {
      buff.appendBuffer(ServerUtils.serializeObject(metaData));
    }
    response.write(buff);
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeRow(
      final KeyValueMetadata<List<?>, GenericRow> keyValueMetadata
  ) {
    final KeyValue<List<?>, GenericRow> keyValue = keyValueMetadata.getKeyValue();
    if (keyValue.value() == null) {
      LOG.warn("Dropped tombstone. Not currently supported");
    } else {
      if (mediaType.equals(KsqlMediaType.KSQL_V1_PROTOBUF.mediaType())) {
        final Struct ksqlRecord = new Struct(connectSchema);
        int i = 0;
        for (Field field : connectSchema.fields()) {
          ksqlRecord.put(
                  field,
                  keyValue.value().get(i));
          i += 1;
        }
        final byte[] protoMessage = serializer.serialize("", ksqlRecord);
        final Optional<StreamedRow.DataRowProtobuf> protoRow =
                StreamedRow.pullRowProtobuf(protoMessage).getRowProtobuf();
        writeBuffer(serializeObject(protoRow));
      } else {
        writeBuffer(ServerUtils.serializeObject(keyValue.value().values()));
      }

    }
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
  public QueryStreamResponseWriter writeCompletionMessage() {
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
    response.write("]").end();
  }

  @VisibleForTesting
  static String logicalToProtoSchema(final LogicalSchema schema) {
    final ConnectSchema connectSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());

    final ProtobufSchema protobufSchema = new ProtobufData(
            new ProtobufDataConfig(ImmutableMap.of())).fromConnectSchema(connectSchema);
    return protobufSchema.canonicalString();
  }
}
