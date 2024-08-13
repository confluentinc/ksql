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

import org.junit.Test;

public class JsonConcatTest {
  private final JsonConcat udf = new JsonConcat();

  @Test
  public void shouldMerge2Objects() {
    // When:
    final String result = udf.concat("{\"a\": 1}", "{\"b\": 2}");

    // Then:
    assertEquals("{\"a\":1,\"b\":2}", result);
  }

  @Test
  public void shouldMerge2EmptyObjects() {
    // When:
    final String result = udf.concat("{}", "{}");

    // Then:
    assertEquals("{}", result);
  }

  @Test
  public void shouldOverrideWithAttrsFromTheSecondObject() {
    // When:
    final String result = udf.concat("{\"a\": {\"5\": 6}}", "{\"a\": {\"3\": 4}}");

    // Then:
    assertEquals("{\"a\":{\"3\":4}}", result);
  }

  @Test
  public void shouldMerge2Arrays() {
    // When:
    final String result = udf.concat("[1, 2]", "[3, 4]");

    // Then:
    assertEquals("[1,2,3,4]", result);
  }

  @Test
  public void shouldMergeNestedArrays() {
    // When:
    final String result = udf.concat("[1, [2]]", "[[[3]], [[[4]]]]");

    // Then:
    assertEquals("[1,[2],[[3]],[[[4]]]]", result);
  }

  @Test
  public void shouldWrapPrimitivesInArrays() {
    // When:
    final String result = udf.concat("null", "null");

    // Then:
    assertEquals("[null,null]", result);
  }

  @Test
  public void shouldWrapObjectInArray() {
    // When:
    final String result = udf.concat("[1, 2]", "{\"a\": 1}");

    // Then:
    assertEquals("[1,2,{\"a\":1}]", result);
  }

  @Test
  public void shouldMergeEmptyArrays() {
    // When:
    final String result = udf.concat("[]", "[]");

    // Then:
    assertEquals("[]", result);
  }


  @Test
  public void shouldMerge3Args() {
    // When:
    final String result = udf.concat("1", "2", "3");

    // Then:
    assertEquals("[1,2,3]", result);
  }

  @Test
  public void shouldReturnNullIfTheFirstArgIsNull() {
    assertNull(udf.concat(null, "1"));
  }

  @Test
  public void shouldReturnNullIfTheSecondArgIsNull() {
    assertNull(udf.concat("1", null));
  }

  @Test
  public void shouldReturnNullIfArgumentIsNull() {
    assertNull(udf.concat(null));
  }

  @Test
  public void shouldReturnNullIfBothdArgsAreNull() {
    assertNull(udf.concat(null, null));
  }
}