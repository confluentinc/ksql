/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.array;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ArrayDistinctTest {
  private final ArrayDistinct udf = new ArrayDistinct();

  @Test
  public void shouldDistinctArray() {
    final List<String> result = udf.distinct(Arrays.asList("foo", " ", "foo", "bar"));
    assertThat(result, contains("foo", " ", "bar"));
  }

  @Test
  public void shouldNotChangeDistinctArray() {
    final List<String> result = udf.distinct(Arrays.asList("foo", " ", "bar"));
    assertThat(result, contains("foo", " ", "bar"));
  }

  @Test
  public void shouldDistinctIntArray() {
    final List<Integer> result = udf.distinct(Arrays.asList(1, 2, 3, 2, 1));
    assertThat(result, contains(1, 2, 3));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDistinctArrayOfMaps() {
    final Map<String, Integer> map1 = ImmutableMap.of("foo", 1, "bar", 2, "baz", 3);
    final Map<String, Integer> map2 = ImmutableMap.of("foo", 10, "baz", 3);
    final Map<String, Integer> map3 = ImmutableMap.of("foo", 1, "bar", 2, "baz", 3);
    final List<Map<String, Integer>> result = udf.distinct(Arrays.asList(map1, map2, map3));
    assertThat(result, contains(map1, map2));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDistinctArrayOfLists() {
    final List<String> list1 = Arrays.asList("foo", "bar", "baz");
    final List<String> list2 = Arrays.asList("foo", "bar");
    final List<String> list3 = Arrays.asList("foo", "bar", "baz");
    final List<List<String>> result = udf.distinct(Arrays.asList(list1, list2, list3, null));
    assertThat(result, contains(list1, list2, null));
  }

  @Test
  public void shouldReturnEmptyForEmptyInput() {
    final List<Double> result = udf.distinct(new ArrayList<Double>());
    assertThat(result, is(Collections.EMPTY_LIST));
  }

  @Test
  public void shouldReturnNullForNullInput() {
    final List<Double> result = udf.distinct((List<Double>) null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldConsiderNullAsDistinctValue() {
    final List<Object> result = udf.distinct(Arrays.asList(1, 2, 1, null, 2, null, 3, 1));
    assertThat(result, contains(1, 2, null, 3));
  }


}