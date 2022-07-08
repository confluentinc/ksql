/*
 * Copyright 2020 Confluent Inc.
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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class ArraySortTest {

  private final ArraySort udf = new ArraySort();

  @Test
  public void shouldSortBools() {
    final List<Boolean> input = Arrays.asList(true, false, false);
    final List<Boolean> output = udf.arraySortDefault(input);
    assertThat(output, contains(Boolean.FALSE, Boolean.FALSE, Boolean.TRUE));
  }

  @Test
  public void shouldSortInts() {
    final List<Integer> input = Arrays.asList(1, 3, -2);
    final List<Integer> output = udf.arraySortDefault(input);
    assertThat(output, contains(-2, 1, 3));
  }

  @Test
  public void shouldSortIntsAscending() {
    final List<Integer> input = Arrays.asList(1, 3, -2);
    final List<Integer> output = udf.arraySortWithDirection(input, "ascEnDing");
    assertThat(output, contains(-2, 1, 3));
  }

  @Test
  public void shouldReturnNullWithBadSortDirection() {
    final List<Integer> input = Arrays.asList(1, 3, -2);
    final List<Integer> output = udf.arraySortWithDirection(input, "ASCDESC");
    assertThat(output, is(nullValue()));
  }

  @Test
  public void shouldReturnNullWithNullSortDirection() {
    final List<Integer> input = Arrays.asList(1, 3, -2);
    final List<Integer> output = udf.arraySortWithDirection(input, null);
    assertThat(output, is(nullValue()));
  }

  @Test
  public void shouldSortIntsDescending() {
    final List<Integer> input = Arrays.asList(1, 3, -2);
    final List<Integer> output = udf.arraySortWithDirection(input, "DEsc");
    assertThat(output, contains(3, 1, -2));
  }

  @Test
  public void shouldSortBigInts() {
    final List<Long> input = Arrays.asList(1L, 3L, -2L);
    final List<Long> output = udf.arraySortDefault(input);
    assertThat(output, contains(-2L, 1L, 3L));
  }

  @Test
  public void shouldSortDoubles() {
    final List<Double> input =
        Arrays.asList(Double.valueOf(1.1), Double.valueOf(3.1), Double.valueOf(-1.1));
    final List<Double> output = udf.arraySortDefault(input);
    assertThat(output, contains(Double.valueOf(-1.1), Double.valueOf(1.1), Double.valueOf(3.1)));
  }

  @Test
  public void shouldSortStrings() {
    final List<String> input = Arrays.asList("foo", "food", "bar");
    final List<String> output = udf.arraySortDefault(input);
    assertThat(output, contains("bar", "foo", "food"));
  }

  @Test
  public void shouldSortStringsMixedCase() {
    final List<String> input = Arrays.asList("foo", "Food", "bar", "Bar", "Baz");
    final List<String> output = udf.arraySortDefault(input);
    assertThat(output, contains("Bar", "Baz", "Food", "bar", "foo"));
  }

  @Test
  public void shouldSortDecimals() {
    final List<BigDecimal> input =
        Arrays.asList(BigDecimal.valueOf(1.2), BigDecimal.valueOf(1.3), BigDecimal.valueOf(-1.2));
    final List<BigDecimal> output = udf.arraySortDefault(input);
    assertThat(output,
        contains(BigDecimal.valueOf(-1.2), BigDecimal.valueOf(1.2), BigDecimal.valueOf(1.3)));
  }

  @Test
  public void shouldReturnNullForNullInput() {
    assertThat(udf.arraySortDefault((List<String>) null), is(nullValue()));
  }

  @Test
  public void shouldReturnNullForListOfNullInput() {
    final List<Integer> input = Arrays.asList(null, null, null);
    assertThat(udf.arraySortDefault(input),
        contains((Integer) null, (Integer) null, (Integer) null));
  }

  @Test
  public void shouldSortNullsToEnd() {
    final List<String> input = Arrays.asList(null, "foo", null, "bar", null);
    final List<String> output = udf.arraySortDefault(input);
    assertThat(output, contains("bar", "foo", null, null, null));
  }

  @Test
  public void shouldSortNullsToEndDescending() {
    final List<String> input = Arrays.asList(null, "foo", null, "bar", null);
    final List<String> output = udf.arraySortWithDirection(input, "desc");
    assertThat(output, contains("foo", "bar", null, null, null));
  }

}
