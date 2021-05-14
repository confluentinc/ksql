/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.list;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class ArrayContainsTest {

  private final ArrayContains udf = new ArrayContains();

  @Test
  public void shouldReturnFalseOnEmptyList() {
    assertFalse(udf.contains(Collections.emptyList(), true));
    assertFalse(udf.contains(Collections.emptyList(), false));
    assertFalse(udf.contains(Collections.emptyList(), null));
    assertFalse(udf.contains(Collections.emptyList(), 1.0));
    assertFalse(udf.contains(Collections.emptyList(), 100));
    assertFalse(udf.contains(Collections.emptyList(), "abc"));
    assertFalse(udf.contains(Collections.emptyList(), ""));
  }

  @Test
  public void shouldNotFindValuesInNullListElements() {
    assertTrue(udf.contains(Collections.singletonList(null), null));
    assertFalse(udf.contains(Collections.singletonList(null), "null"));
    assertFalse(udf.contains(Collections.singletonList(null), true));
    assertFalse(udf.contains(Collections.singletonList(null), false));
    assertFalse(udf.contains(Collections.singletonList(null), 1.0));
    assertFalse(udf.contains(Collections.singletonList(null), 100));
    assertFalse(udf.contains(Collections.singletonList(null), "abc"));
    assertFalse(udf.contains(Collections.singletonList(null), ""));
    assertFalse(udf.contains(null, "null"));
  }

  @Test
  public void shouldFindStringInList() {
    assertTrue(udf.contains(Arrays.asList("abc", "bd", "DC"), "DC"));
    assertFalse(udf.contains(Arrays.asList("abc", "bd", "DC"), "dc"));
    assertFalse(udf.contains(Arrays.asList("abc", "bd", "1"), 1));
  }

  @Test
  public void shouldFindIntegersInList() {
    assertTrue(udf.contains(Arrays.asList(1, 2, 3), 2));
    assertFalse(udf.contains(Arrays.asList(1, 2, 3), 0));
    assertFalse(udf.contains(Arrays.asList(1, 2, 3), "1"));
    assertFalse(udf.contains(Arrays.asList(1, 2, 3), "aa"));
  }

  @Test
  public void shouldFindLongInList() {
    assertTrue(udf.contains(Arrays.asList(1L, 2L, 3L), 2L));
    assertFalse(udf.contains(Arrays.asList(1L, 2L, 3L), 0L));
    assertFalse(udf.contains(Arrays.asList(1L, 2L, 3L), "1"));
    assertFalse(udf.contains(Arrays.asList(1L, 2L, 3L), "aaa"));
  }

  @Test
  public void shouldFindDoublesInList() {
    assertTrue(udf.contains(Arrays.asList(1.0, 2.0, 3.0), 2.0));
    assertFalse(udf.contains(Arrays.asList(1.0, 2.0, 3.0), 4.0));
    assertFalse(udf.contains(Arrays.asList(1.0, 2.0, 3.0), "1"));
    assertFalse(udf.contains(Arrays.asList(1.0, 2.0, 3.0), "aaa"));
  }

}