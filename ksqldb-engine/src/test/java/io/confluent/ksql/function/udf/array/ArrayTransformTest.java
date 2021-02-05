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

package io.confluent.ksql.function.udf.array;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;

public class ArrayTransformTest {

  private ArrayTransform udf;

  @Before
  public void setUp() {
    udf = new ArrayTransform();
  }

  @Test
  public void shouldReturnNullForNullArray() {
    assertThat(udf.arrayTransform(null, function1()), is(nullValue()));
  }

  @Test
  public void shouldApplyFunctionToEachElement() {
    assertThat(udf.arrayTransform(Collections.emptyList(), function1()), is(Collections.emptyList()));
    assertThat(udf.arrayTransform(Arrays.asList(-5, -2, 0), function1()), is(Arrays.asList(0, 3, 5)));
    assertThat(udf.arrayTransform(Arrays.asList(3, null, 5), function1()), is(Arrays.asList(8, null, 10)));

    assertThat(udf.arrayTransform(Collections.emptyList(), function2()), is(Collections.emptyList()));
    assertThat(udf.arrayTransform(Arrays.asList(-5, -2, 0), function2()), is(Arrays.asList("odd", "even", "even")));
    assertThat(udf.arrayTransform(Arrays.asList(3, null, 5), function2()), is(Arrays.asList("odd", null, "odd")));

    assertThat(udf.arrayTransform(Collections.emptyList(), function3()), is(Collections.emptyList()));
    assertThat(udf.arrayTransform(Arrays.asList("steven", "leah"), function3()), is(Arrays.asList("hello steven", "hello leah")));
    assertThat(udf.arrayTransform(Arrays.asList("rohan", null, "almog"), function3()), is(Arrays.asList("hello rohan", null, "hello almog")));

    assertThat(udf.arrayTransform(Collections.emptyList(), function4()), is(Collections.emptyList()));
    assertThat(udf.arrayTransform(Arrays.asList(Arrays.asList(5, 4 ,3), Collections.emptyList()), function4()), is(Arrays.asList(3, 0)));
    assertThat(udf.arrayTransform(Arrays.asList(Arrays.asList(334, 1), null), function4()), is(Arrays.asList(2, null)));
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
}