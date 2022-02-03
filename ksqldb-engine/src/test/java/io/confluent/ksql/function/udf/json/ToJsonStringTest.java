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

package io.confluent.ksql.function.udf.json;

import static org.junit.Assert.assertEquals;

import io.confluent.ksql.schema.ksql.SqlTimeTypes;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class ToJsonStringTest {
  private final ToJsonString udf = new ToJsonString();

  @Test
  public void shouldSerializeInt() {
    // When:
    final String result = udf.toJsonString(123);

    // Then:
    assertEquals("123", result);
  }

  @Test
  public void shouldSerializeBoolean() {
    // When:
    final String result = udf.toJsonString(true);

    // Then:
    assertEquals("true", result);
  }

  @Test
  public void shouldSerializeLong() {
    // When:
    final String result = udf.toJsonString(123L);

    // Then:
    assertEquals("123", result);
  }

  @Test
  public void shouldSerializeDouble() {
    // When:
    final String result = udf.toJsonString(123.456d);

    // Then:
    assertEquals("123.456", result);
  }

  @Test
  public void shouldSerializeDecimal() {
    // When:
    final String result = udf.toJsonString(new BigDecimal("0.33333"));

    // Then:
    assertEquals("0.33333", result);
  }

  @Test
  public void shouldSerializeString() {
    // When:
    final String result = udf.toJsonString("abc");

    // Then:
    assertEquals("\"abc\"", result);
  }

  @Test
  public void shouldSerializeBytes() {
    // When:
    final String result = udf.toJsonString(ByteBuffer.allocate(4).putInt(1097151));

    // Then:
    assertEquals("\"ABC9vw==\"", result);
  }

  @Test
  public void shouldSerializeDate() {
    // When:
    final String result = udf.toJsonString(SqlTimeTypes.parseDate("2022-01-20"));

    // Then:
    assertEquals("\"2022-01-20\"", result);
  }

  @Test
  public void shouldSerializeTime() {
    // When:
    final String result = udf.toJsonString(SqlTimeTypes.parseTime("06:02:20.588"));

    // Then:
    assertEquals("\"06:02:20\"", result);
  }

  @Test
  public void shouldSerializeTimestamp() {
    // When:
    final String result = udf.toJsonString(SqlTimeTypes.parseTimestamp("2022-01-20T17:06:02.588"));

    // Then:
    assertEquals("\"2022-01-20T17:06:02.588\"", result);
  }

  @Test
  public void shouldSerializeNull() {
    // When:
    final String result = udf.toJsonString((Integer)null);

    // Then:
    assertEquals("null", result);
  }

  @Test
  public void shouldSerializeArray() {
    // When:
    final String result = udf.toJsonString(Arrays.asList(1, 2, 3));

    // Then:
    assertEquals("[1,2,3]", result);
  }

  @Test
  public void shouldSerializeStruct() {
    // When:
    final Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();
    final Struct struct = new Struct(schema);
    struct.put("id", 1);
    struct.put("name", "Alice");
    final String result = udf.toJsonString(struct);

    // Then:
    assertEquals("{\"id\":1,\"name\":\"Alice\"}", result);
  }

  @Test
  public void shouldSerializeMap() {
    // When:
    final Map<String, Integer> map = new HashMap<String, Integer>() {{
        put("c", 2);
        put("d", 4);
      }};
    final String result = udf.toJsonString(map);

    // Then:
    assertEquals("{\"c\":2,\"d\":4}", result);
  }

  @Test
  public void shouldSerializeNestedArrayOfStructs() {
    // When:
    final Schema mapSchema = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA);
    final String structkey = "json_key";
    final String structValue = "json_value";
    final Schema schema = SchemaBuilder.struct()
        .field(structkey, Schema.INT32_SCHEMA)
        .field(structValue, mapSchema)
        .build();
    final Struct struct = new Struct(schema);
    struct.put(structkey, 1);
    struct.put(structValue, new HashMap<Integer, String>() {{
        put(2, "c");
        put(4, "d");
      }}
    );
    final String result = udf.toJsonString(Collections.singletonList(struct));

    // Then:
    assertEquals("[{\"json_key\":1,\"json_value\":{\"2\":\"c\",\"4\":\"d\"}}]", result);
  }
}