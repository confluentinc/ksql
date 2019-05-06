/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.connect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class DefaultSchemaFormatterTest {

  private static final Schema OPTIONAL_PRIMITIVES = SchemaBuilder.struct()
      .field("a_boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("an_int8", Schema.OPTIONAL_INT8_SCHEMA)
      .field("an_int16", Schema.OPTIONAL_INT16_SCHEMA)
      .field("an_int32", Schema.OPTIONAL_INT32_SCHEMA)
      .field("an_int64", Schema.OPTIONAL_INT64_SCHEMA)
      .field("a_float32", Schema.OPTIONAL_FLOAT32_SCHEMA)
      .field("a_float64", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("a_bytes", Schema.OPTIONAL_BYTES_SCHEMA)
      .field("a_string", Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build();

  private static final Schema PRIMITIVES = SchemaBuilder.struct()
      .field("a_boolean", Schema.BOOLEAN_SCHEMA)
      .field("an_int8", Schema.INT8_SCHEMA)
      .field("an_int16", Schema.INT16_SCHEMA)
      .field("an_int32", Schema.INT32_SCHEMA)
      .field("an_int64", Schema.INT64_SCHEMA)
      .field("a_float32", Schema.FLOAT32_SCHEMA)
      .field("a_float64", Schema.FLOAT64_SCHEMA)
      .field("a_bytes", Schema.BYTES_SCHEMA)
      .field("a_string", Schema.STRING_SCHEMA)
      .build();

  private static final Schema SCHEMA = SchemaBuilder.struct()
      .field("optional-primitives", OPTIONAL_PRIMITIVES)
      .field("required-primitives", PRIMITIVES)
      .field("array_1", SchemaBuilder.array(OPTIONAL_PRIMITIVES).build())
      .field("array_2", SchemaBuilder.array(PRIMITIVES).build())
      .field("map_1", SchemaBuilder.map(OPTIONAL_PRIMITIVES, PRIMITIVES).build())
      .field("map_2", SchemaBuilder.map(PRIMITIVES, OPTIONAL_PRIMITIVES).build())
      .build();

  @Test
  public void shouldFormatSchema() {
    // When:
    final String result = new DefaultSchemaFormatter().format(SCHEMA);

    // Then:
    final String optionalPrimitivesStruct = "optional<struct<"
        + "a_boolean optional<boolean>,"
        + "an_int8 optional<int8>,"
        + "an_int16 optional<int16>,"
        + "an_int32 optional<int32>,"
        + "an_int64 optional<int64>,"
        + "a_float32 optional<float32>,"
        + "a_float64 optional<float64>,"
        + "a_bytes optional<bytes>,"
        + "a_string optional<string>"
        + ">>";

    final String primitives = "struct<"
        + "a_boolean boolean,"
        + "an_int8 int8,"
        + "an_int16 int16,"
        + "an_int32 int32,"
        + "an_int64 int64,"
        + "a_float32 float32,"
        + "a_float64 float64,"
        + "a_bytes bytes,"
        + "a_string string"
        + ">";

    assertThat(result, is(
        "struct<"
            + "optional-primitives " + optionalPrimitivesStruct + ","
            + "required-primitives " + primitives + ","
            + "array_1 array<" + optionalPrimitivesStruct + ">,"
            + "array_2 array<" + primitives + ">,"
            + "map_1 map<" + optionalPrimitivesStruct + ", " + primitives + ">,"
            + "map_2 map<" + primitives + ", " + optionalPrimitivesStruct + ">"
            + ">"));
  }
}