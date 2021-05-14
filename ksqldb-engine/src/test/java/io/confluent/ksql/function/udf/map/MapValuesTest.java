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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class MapValuesTest {

  private MapValues udf;

  @Before
  public void setUp() {
    udf = new MapValues();
  }

  @Test
  public void shouldGetKeys() {
    final Map<String, String> input = new HashMap<>();
    input.put("foo", "spam");
    input.put("bar", "baloney");
    assertThat(udf.mapValues(input), containsInAnyOrder("spam", "baloney"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldHandleComplexValueTypes() {
    final Map<String, Map<String, List<Double>>> input = Maps.newHashMap();

    final Map<String, List<Double>> entry1 = Maps.newHashMap();
    entry1.put("apple", Arrays.asList(Double.valueOf(12.34), Double.valueOf(56.78)));
    entry1.put("banana", Arrays.asList(Double.valueOf(43.21), Double.valueOf(87.65)));
    input.put("foo", entry1);

    final Map<String, List<Double>> entry2 = Maps.newHashMap();
    entry2.put("cherry", Arrays.asList(Double.valueOf(12.34), Double.valueOf(56.78)));
    entry2.put("date", Arrays.asList(Double.valueOf(43.21), Double.valueOf(87.65)));
    input.put("bar", entry2);

    List<Map<String, List<Double>>> values = udf.mapValues(input);
    assertThat(values, containsInAnyOrder(entry1, entry2));
  }

  @Test
  public void shouldReturnNullForNullInput() {
    List<Long> result = udf.mapValues((Map<String, Long>) null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullsFromMapWithNulls() {
    final Map<String, Integer> input = Maps.newHashMap();
    input.put("foo", 1);
    input.put(null, null);
    input.put("bar", null);
    List<Integer> result = udf.mapValues(input);
    assertThat(result, containsInAnyOrder(1, null, null));
  }

  @Test
  public void shouldReturnEmptyListFromEmptyMap() {
    final Map<String, BigDecimal> input = Maps.newHashMap();
    assertThat(udf.mapValues(input), empty());
  }

}