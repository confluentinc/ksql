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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.codegen.helpers.TriFunction;
import io.confluent.ksql.function.GenericsUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Test;

public class ReduceTest {

  private Reduce udf;

  @Before
  public void setUp() {
    udf = new Reduce();
  }

  @Test
  public void shouldReturnOriginalStateForNullCollection() {
    assertThat(udf.reduceMap(0, null, triFunction1()), is(0));
    assertThat(udf.reduceArray("", null, biFunction1()), is(""));
  }

  @Test
  public void shouldReturnNullForNullState() {
    assertThat(udf.reduceMap(null, Collections.emptyMap(), triFunction1()), is(nullValue()));
    assertThat(udf.reduceArray(null, Collections.emptyList(), biFunction1()), is(nullValue()));
  }

  @Test
  public void shouldReduceMap() {
    final Map<Integer, Integer> map1 = new HashMap<>();
    assertThat(udf.reduceMap(3, map1, triFunction1()), is(3));
    map1.put(4, 3);
    map1.put(6, 2);
    assertThat(udf.reduceMap(42, map1,triFunction1()), is(57));
    assertThat(udf.reduceMap(-4, map1, triFunction1()), is(11));
    map1.put(0,0);
    assertThat(udf.reduceMap(0, map1, triFunction1()), is(15));

    final Map<String, Integer> map2 = new HashMap<>();
    assertThat(udf.reduceMap("", map2, triFunction2()), is(""));
    map2.put("a", 42);
    map2.put("b", 11);
    assertThat(udf.reduceMap("", map2, triFunction2()), is("ba"));
    assertThat(udf.reduceMap("string", map2, triFunction2()), is("bastring"));
    map2.put("c",0);
    map2.put("d",15);
    map2.put("e",-5);
    assertThat(udf.reduceMap("q", map2, triFunction2()), is("dbaq"));
  }

  @Test
  public void shouldReduceArray() {
    assertThat(udf.reduceArray("", ImmutableList.of(), biFunction1()), is(""));
    assertThat(udf.reduceArray("answer", ImmutableList.of(), biFunction1()), is("answer"));
    assertThat(udf.reduceArray("", ImmutableList.of(2, 3, 4, 4, 1000), biFunction1()), is("evenoddeveneveneven"));
    assertThat(udf.reduceArray("This is: ", ImmutableList.of(3, -1, -5), biFunction1()), is("This is: oddoddodd"));

    assertThat(udf.reduceArray(0, ImmutableList.of(), biFunction2()), is(0));
    assertThat(udf.reduceArray(14, Arrays.asList(-1, -13), biFunction2()), is(0));
    assertThat(udf.reduceArray(1, ImmutableList.of(-5, 10), biFunction2()), is(6));
    assertThat(udf.reduceArray(-100, ImmutableList.of(100, 1000, 42), biFunction2()), is(1042));
  }

  @Test
  public void shouldNotSkipNullValuesWhenReducing() {
    assertThrows(
        NullPointerException.class,
        () -> udf.reduceArray(0, Collections.singletonList(null), biFunction2())
    );
    assertThrows(
        NullPointerException.class,
        () -> udf.reduceArray(14, Arrays.asList(-1, -13, null),  biFunction2())
    );

    final Map<Integer, Integer> map1 = new HashMap<>();
    map1.put(4, 3);
    map1.put(6, null);
    assertThrows(
        NullPointerException.class,
        () -> udf.reduceMap(3, map1, triFunction1())
    );
  }

  private TriFunction<Integer, Integer, Integer, Integer> triFunction1() {
    return (x,y,z) -> x + y + z;
  }

  private TriFunction<String, String, Integer, String> triFunction2() {
    return (x, y, z) -> {
      if(z - 10 > 0) {
        return y.concat(x);
      }
      return x;
    };
  }

  private BiFunction<String, Integer, String> biFunction1() {
    return (x,y) -> {
      if (y % 2 == 0) {
        return x.concat("even");
      } else {
        return x.concat("odd");
      }
    };
  }

  private BiFunction<Integer, Integer, Integer> biFunction2() {
    return Integer::sum;
  }
}