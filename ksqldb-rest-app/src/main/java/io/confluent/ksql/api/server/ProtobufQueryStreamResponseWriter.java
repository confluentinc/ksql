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
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamedRow.DataRowProtobuf;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.protobuf.ProtobufNoSRSerdeFactory;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.http.HttpServerResponse;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufQueryStreamResponseWriter implements QueryStreamResponseWriter {

  private static final Logger LOG
          = LoggerFactory.getLogger(ProtobufQueryStreamResponseWriter.class);

  private final HttpServerResponse response;
  private final Optional<String> completionMessage;
  private final Optional<String> limitMessage;
  private ConnectSchema connectSchema;
  private KsqlConnectSerializer<Struct> serializer;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public ProtobufQueryStreamResponseWriter(final HttpServerResponse response,
                                           final Optional<String> completionMessage,
                                           final Optional<String> limitMessage) {
    this.response = response;
    this.completionMessage = completionMessage;
    this.limitMessage = limitMessage;
  }

  @Override
  public QueryStreamResponseWriter writeMetadata(final QueryResponseMetadata metaData) {
    final LogicalSchema schema = metaData.schema;
    final String queryId = metaData.queryId;

    connectSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());
    serializer = new ProtobufNoSRSerdeFactory(ImmutableMap.of())
            .createSerializer(connectSchema, Struct.class, false);

    final StreamedRow header = StreamedRow.headerProtobuf(
            new QueryId(queryId), logicalToProtoSchema(schema));

    response.write("[").write(serializeObject(header));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeRow(
          final KeyValueMetadata<List<?>, GenericRow> row
  ) {
    final KeyValue<List<?>, GenericRow> keyValue = row.getKeyValue();
    if (keyValue.value() == null) {
      LOG.warn("Dropped tombstone. Not currently supported");
    } else {
      final Struct ksqlRecord = new Struct(connectSchema);
      int i = 0;
      for (Field field : connectSchema.fields()) {
        ksqlRecord.put(
                field,
                keyValue.value().get(i));
        i += 1;
      }
      final byte[] protoMessage = serializer.serialize("", ksqlRecord);
      final Optional<DataRowProtobuf> protoRow =
              StreamedRow.pullRowProtobuf(protoMessage).getRowProtobuf();
      response.write(",\n").write(serializeObject(protoRow));
    }
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeContinuationToken(
          final PushContinuationToken pushContinuationToken
  ) {
    final StreamedRow streamedRow = StreamedRow.continuationToken(pushContinuationToken);
    response.write(",\n").write(serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeError(final KsqlErrorMessage error) {
    final StreamedRow streamedRow = StreamedRow.error(error);
    response.write(",\n").write(serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeConsistencyToken(
          final ConsistencyToken consistencyToken
  ) {
    final StreamedRow streamedRow = StreamedRow.consistencyToken(consistencyToken);
    response.write(",\n").write(serializeObject(streamedRow));
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeCompletionMessage() {
    if (completionMessage.isPresent()) {
      final StreamedRow streamedRow = StreamedRow.finalMessage(completionMessage.get());
      response.write(",\n").write(serializeObject(streamedRow));
    }
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeLimitMessage() {
    if (limitMessage.isPresent()) {
      final StreamedRow streamedRow = StreamedRow.finalMessage(limitMessage.get());
      response.write(",\n").write(serializeObject(streamedRow));
    }
    return this;
  }

  @Override
  public void end() {
    response.write("]");
    response.end();
  }

  @VisibleForTesting
  static String logicalToProtoSchema(final LogicalSchema schema) {
    final ConnectSchema connectSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());

    final ProtobufSchema protobufSchema = new ProtobufData(
            new ProtobufDataConfig(ImmutableMap.of())).fromConnectSchema(connectSchema);
    return protobufSchema.canonicalString();
  }
}
