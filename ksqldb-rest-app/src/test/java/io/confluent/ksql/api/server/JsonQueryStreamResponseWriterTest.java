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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JsonQueryStreamResponseWriterTest {
  private static final String QUERY_ID = "queryId";
  private static final List<String> COL_NAMES = ImmutableList.of(
          "A", "B", "C"
  );
  private static final List<String> COL_TYPES = ImmutableList.of(
          "INTEGER", "DOUBLE", "ARRAY"
  );
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
          .keyColumn(ColumnName.of("A"), SqlTypes.INTEGER)
          .valueColumn(ColumnName.of("B"), SqlTypes.DOUBLE)
          .valueColumn(ColumnName.of("C"), SqlTypes.array(SqlTypes.STRING))
          .build();

  @Mock
  private HttpServerResponse response;

  private JsonQueryStreamResponseWriter writer;
  private final StringBuilder stringBuilder = new StringBuilder();

  public JsonQueryStreamResponseWriterTest() {
  }

  @Before
  public void setUp() {
    when(response.write((Buffer) any())).thenAnswer(a -> {
      final Buffer buffer = a.getArgument(0);
      stringBuilder.append(new String(buffer.getBytes(), StandardCharsets.UTF_8));
      return response;
    });
    when(response.write((String) any())).thenAnswer(a -> {
      final String str = a.getArgument(0);
      stringBuilder.append(str);
      return response;
    });

    writer = new JsonQueryStreamResponseWriter(response, KsqlMediaType.KSQL_V1_PROTOBUF.mediaType());
  }

  @Test
  public void shouldConvertLogicalSchemaToProtobufSchema() {
    // Given:
    final String expectedProtoSchemaString = "syntax = \"proto3\";\n" +
            "\n" +
            "message ConnectDefault1 {\n" +
            "  int32 A = 1;\n" +
            "  double B = 2;\n" +
            "  repeated string C = 3;\n" +
            "}\n";

    // When:
    final String protoSchema = JsonQueryStreamResponseWriter.logicalToProtoSchema(SCHEMA);

    // Then:
    assertThat(protoSchema, is(expectedProtoSchemaString));
  }

  @Test
  public void shouldSucceedWithNoRows() {
    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
            is("[{\"header\":{\"queryId\":\"queryId\"," +
                    "\"protoSchema\":" +
                    "\"syntax = \\\"proto3\\\";\\n" +
                    "\\n" +
                    "message ConnectDefault1 {\\n" +
                    "  int32 A = 1;\\n" +
                    "  double B = 2;\\n" +
                    "  repeated string C = 3;\\n" +
                    "}\\n" +
                    "\"}}" +
                    "]"));
    verify(response, times(1)).write((Buffer) any());
  }

  @Test
  public void shouldSucceedWithTwoRows() {
    // Given:

    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(123, 234.0d, ImmutableList.of("hello")))));
    writer.writeRow(new KeyValueMetadata<>(
            KeyValue.keyValue(null, GenericRow.genericRow(456, 789.0d, ImmutableList.of("bye")))));
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
            is("[{\"header\":{\"queryId\":\"queryId\"," +
                    "\"protoSchema\":" +
                    "\"syntax = \\\"proto3\\\";\\n" +
                    "\\n" +
                    "message ConnectDefault1 {\\n" +
                    "  int32 A = 1;\\n" +
                    "  double B = 2;\\n" +
                    "  repeated string C = 3;\\n" +
                    "}\\n" +
                    "\"}}," +
                    "{\"row\":\"CHsRAAAAAABAbUAaBWhlbGxv\"}," +
                    "{\"row\":\"CMgDEQAAAAAAqIhAGgNieWU=\"}" +
                    "]"));
    verify(response, times(3)).write((Buffer) any());
  }
}
