/*
 * Copyright 2019 Confluent Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class ArrayContainsTest {

  private ArrayContains jsonUdf = new ArrayContains();

  @Test
  public void shouldReturnFalseOnEmptyList() {
    assertFalse(jsonUdf.contains(Collections.emptyList(), true));
    assertFalse(jsonUdf.contains(Collections.emptyList(), false));
    assertFalse(jsonUdf.contains(Collections.emptyList(), null));
    assertFalse(jsonUdf.contains(Collections.emptyList(), 1.0));
    assertFalse(jsonUdf.contains(Collections.emptyList(), 100));
    assertFalse(jsonUdf.contains(Collections.emptyList(), "abc"));
    assertFalse(jsonUdf.contains(Collections.emptyList(), ""));
  }

  @Test
  public void shouldNotFindValuesInNullListElements() {
    assertTrue(jsonUdf.contains(Collections.singletonList(null), null));
    assertFalse(jsonUdf.contains(Collections.singletonList(null), "null"));
    assertFalse(jsonUdf.contains(Collections.singletonList(null), true));
    assertFalse(jsonUdf.contains(Collections.singletonList(null), false));
    assertFalse(jsonUdf.contains(Collections.singletonList(null), 1.0));
    assertFalse(jsonUdf.contains(Collections.singletonList(null), 100));
    assertFalse(jsonUdf.contains(Collections.singletonList(null), "abc"));
    assertFalse(jsonUdf.contains(Collections.singletonList(null), ""));
    assertFalse(jsonUdf.contains(null, "null"));
  }

  @Test
  public void shouldFindStringInList() {
    assertTrue(jsonUdf.contains(Arrays.asList("abc", "bd", "DC"), "DC"));
    assertFalse(jsonUdf.contains(Arrays.asList("abc", "bd", "DC"), "dc"));
    assertFalse(jsonUdf.contains(Arrays.asList("abc", "bd", "1"), 1));
  }

  @Test
  public void shouldFindIntegersInList() {
    assertTrue(jsonUdf.contains(Arrays.asList(1, 2, 3), 2));
    assertFalse(jsonUdf.contains(Arrays.asList(1, 2, 3), 0));
    assertFalse(jsonUdf.contains(Arrays.asList(1, 2, 3), "1"));
    assertFalse(jsonUdf.contains(Arrays.asList(1, 2, 3), "aa"));
  }

  @Test
  public void shouldFindLongInList() {
    assertTrue(jsonUdf.contains(Arrays.asList(1L, 2L, 3L), 2L));
    assertFalse(jsonUdf.contains(Arrays.asList(1L, 2L, 3L), 0L));
    assertFalse(jsonUdf.contains(Arrays.asList(1L, 2L, 3L), "1"));
    assertFalse(jsonUdf.contains(Arrays.asList(1L, 2L, 3L), "aaa"));
  }

  @Test
  public void shouldFindDoublesInList() {
    assertTrue(jsonUdf.contains(Arrays.asList(1.0, 2.0, 3.0), 2.0));
    assertFalse(jsonUdf.contains(Arrays.asList(1.0, 2.0, 3.0), 4.0));
    assertFalse(jsonUdf.contains(Arrays.asList(1.0, 2.0, 3.0), "1"));
    assertFalse(jsonUdf.contains(Arrays.asList(1.0, 2.0, 3.0), "aaa"));
  }

}