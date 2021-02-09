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

package io.confluent.ksql.function.udf.map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

public class MapTransformTest {

  private MapTransform udf;

  @Before
  public void setUp() {
    udf = new MapTransform();
  }

  @Test
  public void shouldReturnNullForNullMap() {
    assertThat(udf.mapTransform(null, biFunction1(), biFunction2()), is(nullValue()));
  }

  @Test
  public void shouldReturnTransformedMap() {
    final Map<Integer, Integer> map1 = new HashMap<>();
    assertThat(udf.mapTransform(map1, biFunction1(), biFunction2()), is(Collections.emptyMap()));
    map1.put(3, 100);
    map1.put(1, -2);
    assertThat(udf.mapTransform(map1, biFunction1(), biFunction2()), is(Stream.of(new Object[][] {
        { -97, 97 },
        { 3, -3 },
    }).collect(Collectors.toMap(data -> (Integer) data[0], data -> (Integer) data[1]))));

    final Map<String, String> map2 = new HashMap<>();
    assertThat(udf.mapTransform(map2, biFunction3(), biFunction4()), is(Collections.emptyMap()));
    map2.put("123", "456789");
    map2.put("hello", "hi");
    assertThat(udf.mapTransform(map2, biFunction3(), biFunction4()), is(Stream.of(new Object[][] {
        { "456789123", false },
        { "hihello", true },
    }).collect(Collectors.toMap(data -> (String) data[0], data -> (Boolean) data[1]))));
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