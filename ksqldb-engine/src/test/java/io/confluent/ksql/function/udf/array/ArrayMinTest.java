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

public class ArrayMinTest {

  private final ArrayMin udf = new ArrayMin();

  @Test
  public void shouldFindBoolMin() {
    final List<Boolean> input = Arrays.asList(true, false, false);
    assertThat(udf.arrayMin(input), is(Boolean.FALSE));
  }

  @Test
  public void shouldFindIntMin() {
    final List<Integer> input = Arrays.asList(1, 3, -2);
    assertThat(udf.arrayMin(input), is(-2));
  }

  @Test
  public void shouldFindBigIntMin() {
    final List<Long> input = Arrays.asList(1L, 3L, -2L);
    assertThat(udf.arrayMin(input), is(Long.valueOf(-2)));
  }

  @Test
  public void shouldFindDoubleMin() {
    final List<Double> input =
        Arrays.asList(Double.valueOf(1.1), Double.valueOf(3.1), Double.valueOf(-1.1));
    assertThat(udf.arrayMin(input), is(Double.valueOf(-1.1)));
  }

  @Test
  public void shouldFindStringMin() {
    final List<String> input = Arrays.asList("foo", "food", "bar");
    assertThat(udf.arrayMin(input), is("bar"));
  }

  @Test
  public void shouldFindStringMinMixedCase() {
    final List<String> input = Arrays.asList("foo", "Food", "bar", "Bar", "Baz");
    assertThat(udf.arrayMin(input), is("Bar"));
  }

  @Test
  public void shouldFindDecimalMin() {
    final List<BigDecimal> input =
        Arrays.asList(BigDecimal.valueOf(1.2), BigDecimal.valueOf(1.3), BigDecimal.valueOf(-1.2));
    assertThat(udf.arrayMin(input), is(BigDecimal.valueOf(-1.2)));
  }

  @Test
  public void shouldReturnNullForNullInput() {
    assertThat(udf.arrayMin((List<String>) null), is(nullValue()));
  }

  @Test
  public void shouldReturnNullForListOfNullInput() {
    final List<Integer> input = Arrays.asList(null, null, null);
    assertThat(udf.arrayMin(input), is(nullValue()));
  }

  @Test
  public void shouldReturnValueForMixedInput() {
    final List<String> input = Arrays.asList(null, "foo", null, "bar", null);
    assertThat(udf.arrayMin(input), is("bar"));
  }

}
