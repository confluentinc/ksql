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

package io.confluent.ksql.function.udf.lambda;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

public class FilterTest {
  private Filter udf;

  @Before public void setUp() {
    udf = new Filter();
  }

  @Test
  public void shouldReturnNullForNullInput() {
    assertThat(udf.filterArray(null, function1()), is(nullValue()));
    assertThat(udf.filterMap(null, biFunction1()), is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullFunction() {
    Function<Integer, Boolean> nullFunction = null;
    assertThat(udf.filterArray(Arrays.asList(1, 2, 3, 4), nullFunction), is(nullValue()));

    BiFunction<Integer, Integer, Boolean> nullBiFunction = null;
    final Map<Integer, Integer> m1 = new HashMap<>();
    m1.put(5, 4);
    m1.put(6, 18);
    m1.put(77, 45);
    assertThat(udf.filterMap(m1, nullBiFunction), is(nullValue()));
  }

  @Test
  public void shouldReturnFilteredArray() {
    assertThat(udf.filterArray(Collections.emptyList(), function1()), is(Collections.emptyList()));
    assertThat(udf.filterArray(Arrays.asList(1, 2, 3, 4), function1()), is(Arrays.asList(1, 3)));
    assertThat(udf.filterArray(Arrays.asList(15, 4, 7, -9), function1()), is(Arrays.asList(15, 7, -9)));

    assertThat(udf.filterArray(Collections.emptyList(), function1()), is(Collections.emptyList()));
    assertThat(udf.filterArray(Arrays.asList("bowow", "hello", "goodbye", "wowowo"), function2()), is(Arrays.asList("bowow", "wowowo")));
    assertThat(udf.filterArray(Arrays.asList("woow", "", "wwow"), function2()), is(Collections.singletonList("wwow")));
  }

  @Test
  public void shouldReturnFilteredMap() {
    final Map<Integer, Integer> m1 = new HashMap<>();
    assertThat(udf.filterMap(m1, biFunction1()), is(Collections.emptyMap()));
    m1.put(5, 4);
    m1.put(6, 18);
    m1.put(77, 45);
    assertThat(udf.filterMap(m1, biFunction1()), is(Stream.of(new Object[][] {
        { 6, 18 },
    }).collect(Collectors.toMap(data -> (Integer) data[0], data -> (Integer) data[1]))));

    final Map<String, String> m2 = new HashMap<>();
    m2.put("yes", "no");
    m2.put("okey", "yeah");
    m2.put("ab", "cd");
    assertThat(udf.filterMap(m2, biFunction2()), is(Stream.of(new Object[][] {
        { "okey", "yeah" },
    }).collect(Collectors.toMap(data -> (String) data[0], data -> (String) data[1]))));

  }

  @Test
  public void shouldThrowErrorOnNullArrayInput() {
    assertThrows(NullPointerException.class, () -> udf.filterArray(Collections.singletonList(null), function1()));
    assertThrows(NullPointerException.class, () -> udf.filterArray(Collections.singletonList(null), function2()));
  }

  @Test
  public void shouldThrowErrorOnNullMapInput() {
    final Map<Integer, Integer> m1 = new HashMap<>();
    m1.put(null, 4);
    final Map<String, String> m2 = new HashMap<>();
    m2.put("nope", null);
    assertThrows(NullPointerException.class, () -> udf.filterMap(m1, biFunction1()));
    assertThrows(NullPointerException.class, () -> udf.filterMap(m2, biFunction2()));
  }

  private Function<Integer, Boolean> function1() {
    return x -> x % 2 != 0;
  }

  private Function<String, Boolean> function2() {
    return x -> x.contains("wow");
  }

  private BiFunction<Integer, Integer, Boolean> biFunction1() {
    return (x, y) -> x % 2 == 0 && y % 2 == 0;
  }

  private BiFunction<String, String, Boolean> biFunction2() {
    return (x, y) -> x.contains("e") && y.contains("a");
  }
}
