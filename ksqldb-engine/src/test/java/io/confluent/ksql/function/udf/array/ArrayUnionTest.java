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
import static org.hamcrest.Matchers.contains;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ArrayUnionTest {

  private final ArrayUnion udf = new ArrayUnion();

  @Test
  public void shouldUnionArraysOfLikeType() {
    final List<String> input1 = Arrays.asList("foo", " ", "bar");
    final List<String> input2 = Arrays.asList("baz");
    final List<String> result = udf.union(input1, input2);
    assertThat(result, contains("foo", " ", "bar", "baz"));
  }

  @Test
  public void shouldReturnDistinctValues() {
    final List<String> input1 = Arrays.asList("foo", "foo", "bar");
    final List<String> input2 = Arrays.asList("baz", "foo");
    final List<String> result = udf.union(input1, input2);
    assertThat(result, contains("foo", "bar", "baz"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldIntersectArraysOfMaps() {
    final Map<String, Integer> map1 = ImmutableMap.of("foo", 1, "bar", 2, "baz", 3);
    final Map<String, Integer> map2 = ImmutableMap.of("foo", 10, "baz", 3);
    final Map<String, Integer> map3 = ImmutableMap.of("foo", 1, "bar", 2, "baz", 3);
    final List<Map<String, Integer>> input1 = Arrays.asList(map1, map2);
    final List<Map<String, Integer>> input2 = Arrays.asList(map2, map3);
    final List<Map<String, Integer>> result = udf.union(input1, input2);
    assertThat(result, contains(map1, map2));
  }

  @Test
  public void shouldUnionArraysContainingNulls() {
    final List<String> input1 = Arrays.asList(null, "bar");
    final List<String> input2 = Arrays.asList("foo");
    final List<String> result = udf.union(input1, input2);
    assertThat(result, contains(null, "bar", "foo"));
  }

  @Test
  public void shouldUnionArraysBothContainingNulls() {
    final List<String> input1 = Arrays.asList(null, "foo", "bar");
    final List<String> input2 = Arrays.asList("foo", null);
    final List<String> result = udf.union(input1, input2);
    assertThat(result, contains((String) null, "foo", "bar"));
  }

  @Test
  public void shouldReturnNullForArraysOfOnlyNulls() {
    final List<String> input1 = Arrays.asList(null, null);
    final List<String> input2 = Arrays.asList(null, null, null);
    final List<String> result = udf.union(input1, input2);
    assertThat(result, contains(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullLeftInput() {
    final List<String> input1 = Arrays.asList("foo");
    final List<String> result = udf.union(input1, null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullRightInput() {
    final List<String> input1 = Arrays.asList("foo");
    final List<String> result = udf.union(null, input1);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForAllNullInputs() {
    final List<Long> result = udf.union((List<Long>) null, (List<Long>) null);
    assertThat(result, is(nullValue()));
  }

}