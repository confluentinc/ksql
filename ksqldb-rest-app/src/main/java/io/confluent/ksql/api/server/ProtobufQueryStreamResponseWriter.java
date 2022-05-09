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
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.http.HttpServerResponse;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public class ProtobufQueryStreamResponseWriter implements QueryStreamResponseWriter {
  private final HttpServerResponse response;
  private ConnectSchema connectSchema;
  private KsqlConnectSerializer<Struct> serializer;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public ProtobufQueryStreamResponseWriter(final HttpServerResponse response) {
    this.response = response;
  }

  @Override
  public QueryStreamResponseWriter writeMetadata(final QueryResponseMetadata metaData) {
    final LogicalSchema schema = metaData.schema;
    final String queryId = metaData.queryId;

    connectSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());
    serializer = new ProtobufNoSRSerdeFactory(ImmutableMap.of())
            .createSerializer(connectSchema, Struct.class, false);

    final ProtobufSchema protobufSchema = new ProtobufData(
            new ProtobufDataConfig(ImmutableMap.of())).fromConnectSchema(connectSchema);

    final StreamedRow header = StreamedRow.headerProtobuf(
            new QueryId(queryId), protobufSchema.canonicalString());

    response.write("[").write(serializeObject(header)).write(",\n");
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeRow(
          final KeyValueMetadata<List<?>, GenericRow> row
  ) {
    final Struct ksqlRecord = new Struct(connectSchema);
    int i = 0;
    for (Field field: connectSchema.fields()) {
      ksqlRecord.put(
              field,
              row.getKeyValue().value().get(i));
      i += 1;
    }
    final byte[] protoMessage = serializer.serialize("", ksqlRecord);
    final Optional<DataRowProtobuf> protoRow =
            StreamedRow.pullRowProtobuf(protoMessage).getRowProtobuf();
    response.write(serializeObject(protoRow)).write(",\n");
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeContinuationToken(
          final PushContinuationToken pushContinuationToken
  ) {
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeError(final KsqlErrorMessage error) {
    return this;
  }

  @Override
  public QueryStreamResponseWriter writeConsistencyToken(
          final ConsistencyToken consistencyToken
  ) {
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

  @Override
  public void end() {
    response.write("]");
    response.end();
  }
}
