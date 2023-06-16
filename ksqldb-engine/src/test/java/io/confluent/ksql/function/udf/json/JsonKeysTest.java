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


import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

import io.confluent.ksql.function.KsqlFunctionException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class JsonKeysTest {

  private static final JsonKeys udf = new JsonKeys();

  @Test
  public void shouldReturnObjectKeys() {
    // When:
    final List<String> result = udf.keys("{\"a\": \"abc\", \"b\": { \"c\": \"a\" }, \"d\": 1}");

    // Then:
    assertEquals(Arrays.asList("a", "b", "d"), result);
  }

  @Test
  public void shouldReturnKeysForEmptyObject() {
    // When:
    final List<String> result = udf.keys("{}");

    // Then:
    assertEquals(Collections.emptyList(), result);
  }

  @Test
  public void shouldReturnNullForArray() {
    assertNull(udf.keys("[]"));
  }

  @Test
  public void shouldReturnNullForNumber() {
    assertNull(udf.keys("123"));
  }

  @Test
  public void shouldReturnNullForNull() {
    assertNull(udf.keys("null"));
  }

  @Test
  public void shouldReturnNullForString() {
    assertNull(udf.keys("\"abc\""));
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldReturnNullForInvalidJson() {
    udf.keys("abc");
  }
}