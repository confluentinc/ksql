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
import static org.junit.Assert.assertNull;

import io.confluent.ksql.function.KsqlFunctionException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class JsonRecordsTest {
  private final JsonRecords udf = new JsonRecords();

  @Test
  public void shouldExtractRecords() {
    // When
    final Map<String, String> result = udf.records("{\"a\": \"abc\", \"b\": { \"c\": \"a\" }, \"d\": 1}");

    // Then:
    final Map<String, String> expected = new HashMap<String, String>() {{
        put("a", "abc");
        put("b", "{\"c\":\"a\"}");
        put("d", "1");
      }};

    assertEquals(expected, result);
  }

  @Test
  public void shouldReturnEmptyMapForEmptyObject() {
    assertEquals(Collections.emptyMap(), udf.records("{}"));
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