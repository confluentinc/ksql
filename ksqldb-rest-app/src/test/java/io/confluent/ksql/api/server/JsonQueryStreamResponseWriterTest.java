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
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
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
  public void shouldConvertComplexLogicalSchemaToProtobufSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
            .keyColumn(ColumnName.of("K"), SqlTypes.struct()
                    .field("F1", SqlTypes.array(SqlTypes.STRING))
                    .build())
            .valueColumn(ColumnName.of("STR"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("LONG"), SqlTypes.BIGINT)
            .valueColumn(ColumnName.of("DEC"), SqlTypes.decimal(4, 2))
            .valueColumn(ColumnName.of("BYTES_"), SqlTypes.BYTES)
            .valueColumn(ColumnName.of("ARRAY"), SqlTypes.array(SqlTypes.STRING))
            .valueColumn(ColumnName.of("MAP"), SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING))
            .valueColumn(ColumnName.of("STRUCT"), SqlTypes.struct().field("F1", SqlTypes.INTEGER).build())
            .valueColumn(ColumnName.of("COMPLEX"), SqlTypes.struct()
                    .field("DECIMAL", SqlTypes.decimal(2, 1))
                    .field("STRUCT", SqlTypes.struct()
                            .field("F1", SqlTypes.STRING)
                            .field("F2", SqlTypes.INTEGER)
                            .build())
                    .field("ARRAY_STRUCT", SqlTypes.array(SqlTypes.struct().field("F1", SqlTypes.STRING).build()))
                    .field("ARRAY_MAP", SqlTypes.array(SqlTypes.map(SqlTypes.STRING, SqlTypes.INTEGER)))
                    .field("MAP_ARRAY", SqlTypes.map(SqlTypes.STRING, SqlTypes.array(SqlTypes.STRING)))
                    .field("MAP_MAP", SqlTypes.map(SqlTypes.STRING,
                            SqlTypes.map(SqlTypes.STRING, SqlTypes.INTEGER)
                    ))
                    .field("MAP_STRUCT", SqlTypes.map(SqlTypes.STRING,
                            SqlTypes.struct().field("F1", SqlTypes.STRING).build()
                    ))
                    .build()
            )
            .valueColumn(ColumnName.of("TIMESTAMP"), SqlTypes.TIMESTAMP)
            .valueColumn(ColumnName.of("DATE"), SqlTypes.DATE)
            .valueColumn(ColumnName.of("TIME"), SqlTypes.TIME)
            .headerColumn(ColumnName.of("HEAD"), Optional.of("h0"))
            .build();

    final String expectedProtoSchemaString = "syntax = \"proto3\";\n" +
            "\n" +
            "import \"confluent/type/decimal.proto\";\n" +
            "import \"google/protobuf/timestamp.proto\";\n" +
            "import \"google/type/date.proto\";\n" +
            "import \"google/type/timeofday.proto\";\n" +
            "\n" +
            "message ConnectDefault1 {\n" +
            "  ConnectDefault2 K = 1;\n" +
            "  string STR = 2;\n" +
            "  int64 LONG = 3;\n" +
            "  confluent.type.Decimal DEC = 4 [(confluent.field_meta) = {\n" +
            "    params: [\n" +
            "      {\n" +
            "        value: \"4\",\n" +
            "        key: \"precision\"\n" +
            "      },\n" +
            "      {\n" +
            "        value: \"2\",\n" +
            "        key: \"scale\"\n" +
            "      }\n" +
            "    ]\n" +
            "  }];\n" +
            "  bytes BYTES_ = 5;\n" +
            "  repeated string ARRAY = 6;\n" +
            "  repeated ConnectDefault3Entry MAP = 7;\n" +
            "  ConnectDefault4 STRUCT = 8;\n" +
            "  ConnectDefault5 COMPLEX = 9;\n" +
            "  google.protobuf.Timestamp TIMESTAMP = 10;\n" +
            "  google.type.Date DATE = 11;\n" +
            "  google.type.TimeOfDay TIME = 12;\n" +
            "  bytes HEAD = 13;\n" +
            "\n" +
            "  message ConnectDefault2 {\n" +
            "    repeated string F1 = 1;\n" +
            "  }\n" +
            "  message ConnectDefault3Entry {\n" +
            "    string key = 1;\n" +
            "    string value = 2;\n" +
            "  }\n" +
            "  message ConnectDefault4 {\n" +
            "    int32 F1 = 1;\n" +
            "  }\n" +
            "  message ConnectDefault5 {\n" +
            "    confluent.type.Decimal DECIMAL = 1 [(confluent.field_meta) = {\n" +
            "      params: [\n" +
            "        {\n" +
            "          value: \"2\",\n" +
            "          key: \"precision\"\n" +
            "        },\n" +
            "        {\n" +
            "          value: \"1\",\n" +
            "          key: \"scale\"\n" +
            "        }\n" +
            "      ]\n" +
            "    }];\n" +
            "    ConnectDefault6 STRUCT = 2;\n" +
            "    repeated ConnectDefault7 ARRAY_STRUCT = 3;\n" +
            "    repeated ConnectDefault8Entry ARRAY_MAP = 4;\n" +
            "    repeated ConnectDefault9Entry MAP_ARRAY = 5;\n" +
            "    repeated ConnectDefault10Entry MAP_MAP = 6;\n" +
            "    repeated ConnectDefault12Entry MAP_STRUCT = 7;\n" +
            "  \n" +
            "    message ConnectDefault6 {\n" +
            "      string F1 = 1;\n" +
            "      int32 F2 = 2;\n" +
            "    }\n" +
            "    message ConnectDefault7 {\n" +
            "      string F1 = 1;\n" +
            "    }\n" +
            "    message ConnectDefault8Entry {\n" +
            "      string key = 1;\n" +
            "      int32 value = 2;\n" +
            "    }\n" +
            "    message ConnectDefault9Entry {\n" +
            "      string key = 1;\n" +
            "      repeated string value = 2;\n" +
            "    }\n" +
            "    message ConnectDefault10Entry {\n" +
            "      string key = 1;\n" +
            "      repeated ConnectDefault11Entry value = 2;\n" +
            "    \n" +
            "      message ConnectDefault11Entry {\n" +
            "        string key = 1;\n" +
            "        int32 value = 2;\n" +
            "      }\n" +
            "    }\n" +
            "    message ConnectDefault12Entry {\n" +
            "      string key = 1;\n" +
            "      ConnectDefault13 value = 2;\n" +
            "    \n" +
            "      message ConnectDefault13 {\n" +
            "        string F1 = 1;\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}\n";

    // When:
    final String protoSchema = JsonQueryStreamResponseWriter.logicalToProtoSchema(schema);

    // Then:
    assertThat(protoSchema, is(expectedProtoSchemaString));
  }

  @Test
  public void shouldFailNestedArraysConvertLogicalSchemaToProtobufSchema() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
            .valueColumn(ColumnName.of("COMPLEX"), SqlTypes.struct()
                    .field("ARRAY_ARRAY", SqlTypes.array(SqlTypes.array(SqlTypes.STRING)))
                    .build()
            )
            .build();

    // When:
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      JsonQueryStreamResponseWriter.logicalToProtoSchema(schema);
    });

    String expectedMessage = "Array cannot be nested";
    String actualMessage = exception.getMessage();

    // Then:
    assertThat(actualMessage.contains(expectedMessage), is(true));
  }

  @Test
  public void shouldSucceedWithNoRows() {
    // When:
    writer.writeMetadata(new QueryResponseMetadata(QUERY_ID, COL_NAMES, COL_TYPES, SCHEMA));
    writer.writeCompletionMessage().end();

    // Then:
    assertThat(stringBuilder.toString(),
            is("[{\"header\":{\"queryId\":\"queryId\"," +
                    "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"," +
                    "\"protoSchema\":\"syntax = \\\"proto3\\\";\\n" +
                    "\\n" +
                    "message ConnectDefault1 {\\n" +
                    "  int32 A = 1;\\n" +
                    "  double B = 2;\\n" +
                    "  repeated string C = 3;\\n" +
                    "}\\n" +
                    "\"}}]"));
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
                    "\"schema\":\"`A` INTEGER KEY, `B` DOUBLE, `C` ARRAY<STRING>\"," +
                    "\"protoSchema\":\"syntax = \\\"proto3\\\";\\n" +
                    "\\n" +
                    "message ConnectDefault1 {\\n" +
                    "  int32 A = 1;\\n" +
                    "  double B = 2;\\n" +
                    "  repeated string C = 3;\\n" +
                    "}\\n" +
                    "\"}}," +
                    "{\"row\":\"CHsRAAAAAABAbUAaBWhlbGxv\"}," +
                    "{\"row\":\"CMgDEQAAAAAAqIhAGgNieWU=\"}]"));

    verify(response, times(3)).write((Buffer) any());
  }
}
