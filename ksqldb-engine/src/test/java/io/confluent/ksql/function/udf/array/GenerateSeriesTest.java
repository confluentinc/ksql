/*
 * Copyright 2019 Confluent Inc.
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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.function.KsqlFunctionException;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class GenerateSeriesTest {

  private final GenerateSeries rangeUdf = new GenerateSeries();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldComputePositiveIntRange() {
    final List<Integer> range = rangeUdf.generateSeriesInt(0, 9);
    assertThat(range, hasSize(10));
    int val = 0;
    for (final Integer i : range) {
      assertThat(val++, is(i));
    }
  }

  @Test
  public void shouldComputeNegativeIntRange() {
    final List<Integer> range = rangeUdf.generateSeriesInt(9, 0);
    assertThat(range, hasSize(10));
    int val = 9;
    for (final Integer i : range) {
      assertThat(val--, is(i));
    }
  }

  @Test
  public void shouldComputeLongRange() {
    final List<Long> range = rangeUdf.generateSeriesLong(0, 9);
    assertThat(range, hasSize(10));
    long val = 0;
    for (final Long i : range) {
      assertThat(val++, is(i));
    }
  }

  @Test
  public void shouldComputeNegativeLongRange() {
    final List<Long> range = rangeUdf.generateSeriesLong(9, 0);
    assertThat(range, hasSize(10));
    long val = 9;
    for (final Long i : range) {
      assertThat(val--, is(i));
    }
  }

  @Test
  public void shouldComputeIntRangeWithPositiveEvenStepInt() {
    final List<Integer> range = rangeUdf.generateSeriesInt(0, 9, 2);
    assertThat(range, hasSize(5));
    int val = 0;
    for (final int i : range) {
      assertThat(val, is(i));
      val += 2;
    }
  }

  @Test
  public void shouldComputeIntRangeWithPositiveOddStepInt() {
    final List<Integer> range = rangeUdf.generateSeriesInt(0, 9, 3);
    assertThat(range, hasSize(4));
    int val = 0;
    for (final int i : range) {
      assertThat(val, is(i));
      val += 3;
    }
  }

  @Test
  public void shouldComputeIntRangeWithNegativeEvenStepInt() {
    final List<Integer> range = rangeUdf.generateSeriesInt(9, 0, -2);
    assertThat(range, hasSize(5));
    int val = 9;
    for (final int i : range) {
      assertThat(val, is(i));
      val -= 2;
    }
  }

  @Test
  public void shouldComputeIntRangeWithNegativeOddStepInt() {
    final List<Integer> range = rangeUdf.generateSeriesInt(9, 0, -3);
    assertThat(range, hasSize(4));
    int val = 9;
    for (final int i : range) {
      assertThat(val, is(i));
      val -= 3;
    }
  }

  @Test
  public void shouldComputeIntRangeWithEvenStepLong() {
    final List<Long> range = rangeUdf.generateSeriesLong(0, 9, 2);
    assertThat(range, hasSize(5));
    long index = 0;
    for (final long i : range) {
      assertThat(index, is(i));
      index += 2;
    }
  }

  @Test
  public void shouldComputeIntRangeWithOddStepLong() {
    final List<Long> range = rangeUdf.generateSeriesLong(0, 9, 3);
    assertThat(range, hasSize(4));
    long index = 0;
    for (final long i : range) {
      assertThat(index, is(i));
      index += 3;
    }
  }

  @Test
  public void shouldComputeIntRangeWithNegativeEvenStepLong() {
    final List<Long> range = rangeUdf.generateSeriesLong(9, 0, -2);
    assertThat(range, hasSize(5));
    long val = 9;
    for (final long i : range) {
      assertThat(val, is(i));
      val -= 2;
    }
  }

  @Test
  public void shouldComputeIntRangeWithNegativeOddStepLong() {
    final List<Long> range = rangeUdf.generateSeriesLong(9, 0, -3);
    assertThat(range, hasSize(4));
    long val = 9;
    for (final long i : range) {
      assertThat(val, is(i));
      val -= 3;
    }
  }

  @Test
  public void shouldThrowOnStepZeroInt() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GENERATE_SERIES step cannot be zero");
    rangeUdf.generateSeriesInt(0, 10, 0);
  }

  @Test
  public void shouldThrowOnStepZeroLong() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GENERATE_SERIES step cannot be zero");
    rangeUdf.generateSeriesLong(0L, 10L, 0);
  }

  @Test
  public void shouldThrowIfStepWrongSignInt1() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GENERATE_SERIES step has wrong sign");
    rangeUdf.generateSeriesInt(0, 10, -1);
  }

  @Test
  public void shouldThrowIfStepWrongSignInt2() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GENERATE_SERIES step has wrong sign");
    rangeUdf.generateSeriesInt(9, 0, 1);
  }

  @Test
  public void shouldThrowIfStepWrongSignLong1() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GENERATE_SERIES step has wrong sign");
    rangeUdf.generateSeriesLong(0, 10, -1);
  }

  @Test
  public void shouldThrowIfStepWrongSignLong2() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GENERATE_SERIES step has wrong sign");
    rangeUdf.generateSeriesLong(9, 0, 1);
  }

}
