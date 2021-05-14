/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.array;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class ArrayMaxTest {

  private final ArrayMax udf = new ArrayMax();

  @Test
  public void shouldFindBoolMax() {
    final List<Boolean> input = Arrays.asList(true, false, false);
    assertThat(udf.arrayMax(input), is(Boolean.TRUE));
  }

  @Test
  public void shouldFindIntMax() {
    final List<Integer> input = Arrays.asList(1, 3, -2);
    assertThat(udf.arrayMax(input), is(3));
  }

  @Test
  public void shouldFindBigIntMax() {
    final List<Long> input = Arrays.asList(1L, 3L, -2L);
    assertThat(udf.arrayMax(input), is(Long.valueOf(3)));
  }

  @Test
  public void shouldFindDoubleMax() {
    final List<Double> input =
        Arrays.asList(Double.valueOf(1.1), Double.valueOf(3.1), Double.valueOf(-1.1));
    assertThat(udf.arrayMax(input), is(Double.valueOf(3.1)));
  }

  @Test
  public void shouldFindStringMax() {
    final List<String> input = Arrays.asList("foo", "food", "bar");
    assertThat(udf.arrayMax(input), is("food"));
  }

  @Test
  public void shouldFindStringMaxMixedCase() {
    final List<String> input = Arrays.asList("foo", "Food", "bar");
    assertThat(udf.arrayMax(input), is("foo"));
  }

  @Test
  public void shouldFindDecimalMax() {
    final List<BigDecimal> input =
        Arrays.asList(BigDecimal.valueOf(1.2), BigDecimal.valueOf(1.3), BigDecimal.valueOf(-1.2));
    assertThat(udf.arrayMax(input), is(BigDecimal.valueOf(1.3)));
  }

  @Test
  public void shouldReturnNullForNullInput() {
    assertThat(udf.arrayMax((List<String>) null), is(nullValue()));
  }

  @Test
  public void shouldReturnNullForListOfNullInput() {
    final List<Integer> input = Arrays.asList(null, null, null);
    assertThat(udf.arrayMax(input), is(nullValue()));
  }

  @Test
  public void shouldReturnValueForMixedInput() {
    final List<String> input = Arrays.asList(null, "foo", null, "bar", null);
    assertThat(udf.arrayMax(input), is("foo"));
  }

}
