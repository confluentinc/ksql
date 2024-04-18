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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.KsqlFunctionException;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class JsonItemsTest {
  private final JsonItems udf = new JsonItems();
  private ObjectMapper mapper = new ObjectMapper();
  @Test
  public void shouldExtractRecords() throws JsonProcessingException {
    // When
    String jsonArrayStr = "[{\"type\": \"AAA\", \"timestamp\": \"2022-01-27\"}, {\"type\": \"BBB\", \"timestamp\": \"2022-05-18\"}]";

    final List<String> result = udf.items(jsonArrayStr);
    // Then:
    final List<String> expected = ImmutableList.of(
        "{\"type\": \"AAA\", \"timestamp\": \"2022-01-27\"}", "{\"type\": \"BBB\", \"timestamp\": \"2022-05-18\"}");


    assertEquals(mapper.readTree(expected.get(0)), mapper.readTree(result.get(0)));
    assertEquals(mapper.readTree(expected.get(1)), mapper.readTree(result.get(1)));
  }

  @Test
  public void shouldReturnEmptyListForEmptyArray() {
    assertEquals(Collections.emptyList(), udf.items("[]"));
  }

  @Test
  public void shouldSupportJsonIntegerType() {
    assertEquals(ImmutableList.of("1","2","3"), udf.items("[1, 2, 3]"));
  }

  @Test
  public void shouldReturnNullForNull() {
    assertNull(udf.items(null));
  }


  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowForNonArray() {
    udf.items("abc");
  }



}