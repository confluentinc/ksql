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

package io.confluent.ksql.rest.client;

import static io.confluent.ksql.rest.client.KsqlClientUtil.deserialize;
import static io.confluent.ksql.util.BytesUtils.toJsonMsg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamedRow.DataRowProtobuf;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.Pair;
import io.vertx.core.buffer.Buffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class KsqlTargetUtil {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private KsqlTargetUtil() {

  }

  // This is meant to parse partial chunk responses as well as full pull query responses.
  public static List<StreamedRow> toRows(final Buffer buff) {

    final List<StreamedRow> rows = new ArrayList<>();
    int begin = 0;

    for (int i = 0; i <= buff.length(); i++) {
      if ((i == buff.length() && (i - begin > 1))
          || (i < buff.length() && buff.getByte(i) == (byte) '\n')) {
        if (begin != i) { // Ignore random newlines - the server can send these
          final Buffer sliced = buff.slice(begin, i);
          final Buffer tidied = toJsonMsg(sliced, true);
          if (tidied.length() > 0) {
            final StreamedRow row = deserialize(tidied, StreamedRow.class);
            rows.add(row);
          }
        }

        begin = i + 1;
      }
    }
    return rows;
  }

  /**
   * @param buff the buffer response that we get from the /query-stream endpoint
   *             for KsqlMediaType.KSQL_V1_PROTOBUF
   * @return a List of StreamedRow that includes the header row as its fist element,
   *     and every subsequent element is a result row in the protobuf format
   */
  public static List<StreamedRow> toRowsFromProto(final Buffer buff) {
    final List<StreamedRow> rows = new ArrayList<>();
    final JsonNode buffRows;
    try {
      buffRows = MAPPER.readTree(buff.toString());
    } catch (JsonProcessingException e) {
      throw new KsqlRestClientException("Failed to deserialize object", e);
    }

    // deserialize the first row (header) into a StreamedRow and add it to
    // the list to return.
    rows.add(deserialize(Buffer.buffer(buffRows.get(0).toString()), StreamedRow.class));
    for (int i = 1; i < buffRows.size(); i++) {
      final Buffer tidied = Buffer.buffer(buffRows.get(i).toString());
      try {
        // try to deserialize every subsequent buffer row into a StreamedRow
        // and add it to the list to return. These can be continuation tokens,
        // consistency tokens, error messages, final messages or limit messages.
        // We expect it to fail for the rowProtobuf objects which is handled below.
        rows.add(deserialize(tidied, StreamedRow.class));
      } catch (Exception e) {
        // if it is none of the above, then deserialize it to a
        // StreamedRow.DataRowProtobuf object.
        // We can't directly deserialize into a StreamedRow like we did above
        // because DataRowProtobuf's serialization is {"row":"CAESBlVTRVJfMA=="}
        // whereas the serialization of a StreamedRow containing a DataRowProtobuf
        // looks like {"rowProtobuf":{"row":"CAESBlVTRVJfMA=="}}. The Rest API
        // produces {"row":"CAESBlVTRVJfMA=="}, so we have to do a bit of extra conversion
        // here.
        final DataRowProtobuf protoBytes =
                deserialize(tidied, DataRowProtobuf.class);
        final byte[] bytes = protoBytes.getRow();
        rows.add(StreamedRow.pullRowProtobuf(bytes));
      }
    }
    return rows;
  }

  public static StreamedRow toRowFromDelimited(final Buffer buff) {
    try {
      final QueryResponseMetadata metadata = deserialize(buff, QueryResponseMetadata.class);
      return StreamedRow.header(new QueryId(Strings.nullToEmpty(metadata.queryId)),
        createSchema(metadata));
    } catch (KsqlRestClientException e) {
      // Not a {@link QueryResponseMetadata}
    }
    try {
      final KsqlErrorMessage error = deserialize(buff, KsqlErrorMessage.class);
      return StreamedRow.error(new RuntimeException(error.getMessage()), error.getErrorCode());
    } catch (KsqlRestClientException e) {
      // Not a {@link KsqlErrorMessage}
    }
    try {
      final PushContinuationToken continuationToken
          = deserialize(buff, PushContinuationToken.class);
      return StreamedRow.continuationToken(continuationToken);
    } catch (KsqlRestClientException e) {
      // Not a {@link KsqlErrorMessage}
    }
    try {
      final List<?> row = deserialize(buff, List.class);
      return StreamedRow.pushRow(GenericRow.fromList(row));
    } catch (KsqlRestClientException e) {
      // Not a {@link List}
    }
    throw new IllegalStateException("Couldn't parse message: " + buff.toString());
  }

  private static LogicalSchema createSchema(final QueryResponseMetadata metadata) {
    final SqlTypeParser parser = SqlTypeParser.create(TypeRegistry.EMPTY);
    return LogicalSchema.builder().valueColumns(
        Streams.zip(metadata.columnNames.stream(), metadata.columnTypes.stream(), Pair::of)
            .map(pair -> {
              final SqlType sqlType = parser.parse(pair.getRight()).getSqlType();
              final ColumnName name = ColumnName.of(pair.getLeft());
              return new SimpleColumn() {
                @Override
                public ColumnName name() {
                  return name;
                }

                @Override
                public SqlType type() {
                  return sqlType;
                }
              };
            }).collect(Collectors.toList()))
        .build();
  }
}
