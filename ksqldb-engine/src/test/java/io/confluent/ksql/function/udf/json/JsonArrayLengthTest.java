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
import org.junit.Test;

public class JsonArrayLengthTest {

  private static final JsonArrayLength udf = new JsonArrayLength();

  @Test
  public void shouldReturnFlatArrayLength() {
    // When:
    final Integer result = udf.length("[1, 2, 3]");

    // Then:
    assertEquals(Integer.valueOf(3), result);
  }

  @Test
  public void shouldReturnNestedArrayLength() {
    // When:
    final Integer result = udf.length("[1, [1, [2]], 3]");

    // Then:
    assertEquals(Integer.valueOf(3), result);
  }

  @Test
  public void shouldReturnEmptyArrayLength() {
    // When:
    final Integer result = udf.length("[]");

    // Then:
    assertEquals(Integer.valueOf(0), result);
  }

  @Test
  public void shouldReturnNullForObjects() {
    // When:
    final Integer result = udf.length("{}");

    // Then:
    assertNull(result);
  }

  @Test
  public void shouldReturnNullForNumber() {
    // When:
    final Integer result = udf.length("123");

    // Then:
    assertNull(result);
  }

  @Test
  public void shouldReturnNullForString() {
    // When:
    final Integer result = udf.length("\"abc\"");

    // Then:
    assertNull(result);
  }

  @Test
  public void shouldReturnNullForNull() {
    // When:
    final Integer result = udf.length(null);

    // Then:
    assertNull(result);
  }

  @Test
  public void shouldReturnNullForStrNull() {
    // When:
    final Integer result = udf.length("null");

    // Then:
    assertNull(result);
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowForInvalidJson() {
    udf.length("abc");
  }
}