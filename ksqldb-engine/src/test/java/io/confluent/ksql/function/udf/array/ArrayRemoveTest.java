/*
 * Copyright 2020 Confluent Inc.
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
import static org.hamcrest.Matchers.hasSize;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ArrayRemoveTest {
  private final ArrayRemove udf = new ArrayRemove();

  @Test
  public void shouldRemoveAllMatchingElements() {
    final List<String> input1 = Arrays.asList("foo", " ", "foo", "bar");
    final String input2 = "foo";
    final List<String> result = udf.remove(input1, input2);
    assertThat(result, is(Arrays.asList(" ", "bar")));
  }

  @Test
  public void shouldRemoveIntegers() {
    final List<Integer> input1 = Arrays.asList(1, 2, 3, 2, 1);
    final Integer input2 = 2;
    final List<Integer> result = udf.remove(input1, input2);
    assertThat(result, contains(1, 3, 1));
  }

  @Test
  public void shouldRemoveDoubles() {
    final List<Double> input1 = Arrays.asList(1.1, 2.99, 1.1, 3.0);
    final Double input2 = 1.1;
    final List<Double> result = udf.remove(input1, input2);
    assertThat(result, contains(2.99, 3.0));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRemoveMap() {
    final Map<String, Integer> map1 = ImmutableMap.of("foo", 1, "bar", 2, "baz", 3);
    final Map<String, Integer> map2 = ImmutableMap.of("foo", 10, "baz", 3);
    final Map<String, Integer> map3 = ImmutableMap.of("foo", 1, "bar", 2, "baz", 3);
    final List<Map<String, Integer>> input1 = Arrays.asList(map1, map2, map3);
    final Map<String, Integer> input2 = map3;
    final List<Map<String, Integer>> result = udf.remove(input1, input2);
    assertThat(result, contains(map2));
  }

  @Test
  public void shouldReturnAllElementsIfNoNull() {
    final List<String> input1 = Arrays.asList("foo");
    final String input2 = null;
    final List<String> result = udf.remove(input1, input2);
    assertThat(result, contains("foo"));
  }

  @Test
  public void shouldReturnAllElementsIfNoMatches() {
    final List<String> input1 = Arrays.asList("foo");
    final String input2 = "bar";
    final List<String> result = udf.remove(input1, input2);
    assertThat(result, contains("foo"));
  }

  @Test
  public void shouldReturnNullForNullInputArray() {
    final List<String> input1 = null;
    final String input2 = "foo";
    final List<String> result = udf.remove(input1, input2);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullInputs() {
    final List<Long> input1 = null;
    final Long input2 = null;
    final List<Long> result = udf.remove(input1, input2);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldRetainNulls() {
    final List<String> input1 = Arrays.asList(null, "foo");
    final String input2 = "foo";
    final List<String> result = udf.remove(input1, input2);
    assertThat(result, contains((String) null));
  }

  @Test
  public void shouldRemoveNullsWhenRequested() {
    final List<String> input1 = Arrays.asList(null, "foo", "bar");
    final String input2 = null;
    final List<String> result = udf.remove(input1, input2);
    assertThat(result, contains("foo", "bar"));
  }

  @Test
  public void shouldReturnEmptyForArraysOfOnlyNulls() {
    final List<String> input1 = Arrays.asList(null, null);
    final String input2 = null;
    final List<String> result = udf.remove(input1, input2);
    assertThat(result, hasSize(0));
  }

}