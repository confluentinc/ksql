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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

public class TransformTest {

  private Transform udf;

  @Before
  public void setUp() {
    udf = new Transform();
  }

  @Test
  public void shouldReturnNullForNullArray() {
    assertThat(udf.transformArray(null, function1()), is(nullValue()));
    assertThat(udf.transformMap(null, biFunction1(), biFunction2()), is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullFunctions() {
    Function<Integer, Integer> nullFunction = null;
    BiFunction<Integer, Integer, Integer> nullBiFunction = null;
    final Map<Integer, Integer> map1 = new HashMap<>();
    map1.put(3, 100);
    map1.put(1, -2);

    assertThat(udf.transformArray(Arrays.asList(-5, -2, 0), nullFunction), is(nullValue()));
    assertThat(udf.transformMap(map1, nullBiFunction, nullBiFunction), is(nullValue()));
  }

  @Test
  public void shouldReturnTransformedArray() {
    assertThat(udf.transformArray(Collections.emptyList(), function1()), is(Collections.emptyList()));
    assertThat(udf.transformArray(Arrays.asList(-5, -2, 0), function1()), is(Arrays.asList(0, 3, 5)));

    assertThat(udf.transformArray(Collections.emptyList(), function2()), is(Collections.emptyList()));
    assertThat(udf.transformArray(Arrays.asList(-5, -2, 0), function2()), is(Arrays.asList("odd", "even", "even")));

    assertThat(udf.transformArray(Collections.emptyList(), function3()), is(Collections.emptyList()));
    assertThat(udf.transformArray(Arrays.asList("steven", "leah"), function3()), is(Arrays.asList("hello steven", "hello leah")));

    assertThat(udf.transformArray(Collections.emptyList(), function4()), is(Collections.emptyList()));
    assertThat(udf.transformArray(Arrays.asList(Arrays.asList(5, 4 ,3), Collections.emptyList()), function4()), is(Arrays.asList(3, 0)));
  }

  @Test
  public void shouldReturnTransformedMap() {
    final Map<Integer, Integer> map1 = new HashMap<>();
    assertThat(udf.transformMap(map1, biFunction1(), biFunction2()), is(Collections.emptyMap()));
    map1.put(3, 100);
    map1.put(1, -2);
    assertThat(udf.transformMap(map1, biFunction1(), biFunction2()), is(Stream.of(new Object[][] {
        { -97, 97 },
        { 3, -3 },
    }).collect(Collectors.toMap(data -> (Integer) data[0], data -> (Integer) data[1]))));

    final Map<String, String> map2 = new HashMap<>();
    assertThat(udf.transformMap(map2, biFunction3(), biFunction4()), is(Collections.emptyMap()));
    map2.put("123", "456789");
    map2.put("hello", "hi");
    assertThat(udf.transformMap(map2, biFunction3(), biFunction4()), is(Stream.of(new Object[][] {
        { "456789123", false },
        { "hihello", true },
    }).collect(Collectors.toMap(data -> (String) data[0], data -> (Boolean) data[1]))));
  }


  @Test
  public void shouldNotSkipNullValuesWhenTransforming() {
    assertThrows(
        NullPointerException.class,
        () -> udf.transformArray(Arrays.asList(Arrays.asList(334, 1), null), function4())
    );
    assertThrows(
        NullPointerException.class,
        () -> udf.transformArray(Arrays.asList("rohan", null, "almog"), function3())
    );
    assertThrows(
        NullPointerException.class,
        () -> udf.transformArray(Arrays.asList(3, null, 5), function2())
    );
    assertThrows(
        NullPointerException.class,
        () -> udf.transformArray(Arrays.asList(3, null, 5), function1())
    );

    final Map<Integer, Integer> map1 = new HashMap<>();
    map1.put(4, 3);
    map1.put(6, null);
    assertThrows(
        NullPointerException.class,
        () -> udf.transformMap(map1, biFunction1(), biFunction2())
    );
  }
  
  private Function<Integer, Integer> function1() {
    return x -> x + 5;
  }

  private Function<Integer, String> function2() {
    return x -> {
      if(x % 2 == 0) {
        return "even";
      } else {
        return "odd";
      }
    };
  }

  private Function<String, String> function3() {
    return "hello "::concat;
  }

  private Function<List<Integer>, Integer> function4() {
    return List::size;
  }

  private BiFunction<Integer, Integer, Integer> biFunction1() {
    return (x,y) -> x - y;
  }

  private BiFunction<Integer, Integer, Integer> biFunction2() {
    return (x,y) -> y - x;
  }

  private BiFunction<String, String, String> biFunction3() {
    return (x,y) -> y.concat(x);
  }

  private BiFunction<String, String, Boolean> biFunction4() {
    return (x,y) -> x.length() > y.length();
  }
}