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

import io.confluent.ksql.execution.codegen.helpers.TriFunction;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class MapReduceTest {

  private MapReduce udf;

  @Before
  public void setUp() {
    udf = new MapReduce();
  }

  @Test
  public void shouldReturnNullForNullMap() {
    assertThat(udf.mapReduce(null, 0, triFunction1()), is(nullValue()));
  }

  @Test
  public void shouldReduceMap() {
    final Map<Integer, Integer> map1 = new HashMap<>();
    assertThat(udf.mapReduce(map1, 3, triFunction1()), is(3));
    map1.put(4, 3);
    map1.put(6, 2);
    assertThat(udf.mapReduce(map1, 42, triFunction1()), is(57));
    assertThat(udf.mapReduce(map1, -4, triFunction1()), is(11));
    map1.put(0,0);
    assertThat(udf.mapReduce(map1, 0, triFunction1()), is(15));

    final Map<String, Integer> map2 = new HashMap<>();
    assertThat(udf.mapReduce(map2, "", triFunction2()), is(""));
    map2.put("a", 42);
    map2.put("b", 11);
    assertThat(udf.mapReduce(map2, "", triFunction2()), is("ba"));
    assertThat(udf.mapReduce(map2, "string", triFunction2()), is("bastring"));
    map2.put("c",0);
    map2.put("d",15);
    map2.put("e",-5);
    assertThat(udf.mapReduce(map2, "q", triFunction2()), is("dbaq"));
  }

  private TriFunction<Integer, Integer, Integer, Integer> triFunction1() {
    return (x,y,z) -> x + y + z;
  }

  private TriFunction<String, Integer, String, String> triFunction2() {
    return (x, y, z) -> {
      if(y - 10 > 0) {
        return x.concat(z);
      }
      return z;
    };
  }
}