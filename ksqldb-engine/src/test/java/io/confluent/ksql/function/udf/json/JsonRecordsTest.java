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

package io.confluent.ksql.function.udf.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.confluent.ksql.function.KsqlFunctionException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class JsonRecordsTest {
  private final JsonRecords udf = new JsonRecords();

  @Test
  public void shouldExtractRecords() {
    // When
    final List<Struct> result = udf.records("{\"a\": \"abc\", \"b\": { \"c\": \"a\" }, \"d\": 1}");

    // Then:
    final Struct s1 = new Struct(JsonRecords.STRUCT_SCHEMA);
    s1.put("JSON_KEY", "a");
    s1.put("JSON_VALUE", "\"abc\"");
    final Struct s2 = new Struct(JsonRecords.STRUCT_SCHEMA);
    s2.put("JSON_KEY", "b");
    s2.put("JSON_VALUE", "{\"c\":\"a\"}");
    final Struct s3 = new Struct(JsonRecords.STRUCT_SCHEMA);
    s3.put("JSON_KEY", "d");
    s3.put("JSON_VALUE", "1");

    assertEquals(Arrays.asList(s1, s2, s3), result);
  }

  @Test
  public void shouldReturnEmptyListForEmptyObject() {
    assertEquals(Collections.emptyList(), udf.records("{}"));
  }

  @Test
  public void shouldReturnNullForJsonNull() {
    assertNull(udf.records("null"));
  }

  @Test
  public void shouldReturnNullForJsonArray() {
    assertNull(udf.records("[1,2,3]"));
  }

  @Test
  public void shouldReturnNullForJsonNumber() {
    assertNull(udf.records("123"));
  }

  @Test
  public void shouldReturnNullForNull() {
    assertNull(udf.records(null));
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowForInvalidJson() {
    udf.records("abc");
  }
}