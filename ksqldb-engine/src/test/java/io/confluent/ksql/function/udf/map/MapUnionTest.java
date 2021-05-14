/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class MapUnionTest {

  private MapUnion udf;

  @Before
  public void setUp() {
    udf = new MapUnion();
  }

  @Test
  public void shouldUnionNonEmptyMaps() {
    final Map<String, String> input1 = Maps.newHashMap();
    input1.put("foo", "spam");
    input1.put("bar", "baloney");

    final Map<String, String> input2 = Maps.newHashMap();
    input2.put("one", "apple");
    input2.put("two", "banana");
    input2.put("three", "cherry");

    final Map<String, String> result = udf.union(input1, input2);
    assertThat(result.size(), is(5));
    assertThat(result.get("foo"), is("spam"));
    assertThat(result.get("two"), is("banana"));
  }

  @Test
  public void shouldReturnNullForNullInput() {
    Map<String, Long> result = udf.union((Map<String, Long>) null, (Map<String, Long>) null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldUnionWithNullMap() {
    final Map<String, Integer> input1 = Maps.newHashMap();
    input1.put("foo", 1);
    input1.put("bar", 2);

    final Map<String, Integer> result = udf.union(input1, null);
    assertThat(result.size(), is(2));
    assertThat(result.get("foo"), is(1));
    assertThat(result.keySet(), containsInAnyOrder("foo", "bar"));
  }

  @Test
  public void shouldHandleComplexValueTypes() {
    final Map<String, List<Double>> input1 = Maps.newHashMap();
    input1.put("apple", Arrays.asList(Double.valueOf(12.34), Double.valueOf(56.78)));
    input1.put("banana", Arrays.asList(Double.valueOf(43.21), Double.valueOf(87.65)));

    final Map<String, List<Double>> input2 = Maps.newHashMap();
    input2.put("foo", Arrays.asList(Double.valueOf(123.456)));

    final Map<String, List<Double>> result = udf.union(input1, input2);
    assertThat(result.size(), is(3));
    assertThat(result.get("banana"), contains(Double.valueOf(43.21), Double.valueOf(87.65)));
    assertThat(result.keySet(), containsInAnyOrder("foo", "banana", "apple"));
  }

  @Test
  public void shouldRetainLatestValueForDuplicateKey() {
    final Map<String, String> input1 = Maps.newHashMap();
    input1.put("foo", "spam");
    input1.put("bar", "baloney");

    final Map<String, String> input2 = Maps.newHashMap();
    input2.put("foo", "apple");
    input2.put("two", "banana");
    input2.put("three", "cherry");

    final Map<String, String> result = udf.union(input1, input2);
    assertThat(result.size(), is(4));
    assertThat(result.get("foo"), is("apple"));
  }

  @Test
  public void shouldUnionMapWithNulls() {
    final Map<String, String> input1 = Maps.newHashMap();
    input1.put("one", "apple");
    input1.put("two", "banana");
    input1.put("three", "cherry");

    final Map<String, String> input2 = Maps.newHashMap();
    input2.put("foo", "bar");
    input2.put(null, null);
    input2.put("baz", null);

    final Map<String, String> result = udf.union(input1, input2);
    assertThat(result.size(), is(6));
    assertThat(result.get("two"), is("banana"));
    assertThat(result.get("foo"), is("bar"));
    assertThat(result.get("baz"), is(nullValue()));
    assertThat(result.keySet(), containsInAnyOrder("one", "two", "three", null, "foo", "baz"));
  }

  @Test
  public void shouldReturnEmptyMapFromEmptyMaps() {
    final Map<String, BigDecimal> input1 = Maps.newHashMap();
    final Map<String, BigDecimal> input2 = Maps.newHashMap();
    assertThat(udf.union(input1, input2), equalTo(Collections.EMPTY_MAP));
  }

}