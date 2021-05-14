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

package io.confluent.ksql.function.udf.string;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;

public class RegexpExtractTest {

  private RegexpExtract udf;

  @Before
  public void setUp() {
    udf = new RegexpExtract();
  }

  @Test
  public void shouldReturnNullOnNullValue() {
    assertNull(udf.regexpExtract(null, null));
    assertNull(udf.regexpExtract(null, null, null));
    assertNull(udf.regexpExtract(null, "", 1));
    assertNull(udf.regexpExtract("some string", null, 1));
    assertNull(udf.regexpExtract("some string", "", null));
  }

  @Test
  public void shouldReturnSubstringWhenMatched() {
    assertEquals(udf.regexpExtract("e.*", "test string"), "est string");
    assertEquals(udf.regexpExtract(".", "test string"), "t");
    assertEquals(udf.regexpExtract("[AEIOU].{4}", "usEr nAme 1"), "Er nA");
  }

  @Test
  public void shouldReturnNullWhenNoMatch() {
    assertNull(udf.regexpExtract("tst", "test string"));
  }

  @Test
  public void shouldReturnSubstringCapturedByGroupNumber() {
    assertEquals(udf.regexpExtract("(.*) (.*)", "test string", 1), "test");
    assertEquals(udf.regexpExtract("(.*) (.*)", "test string", 2), "string");
  }

  @Test
  public void shouldReturnNullIfGivenGroupNumberGreaterThanAvailableGroupNumbers() {
    assertNull(udf.regexpExtract("e", "test string", 3), null);
  }
}