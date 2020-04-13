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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.function.KsqlFunctionException;
import java.util.List;
import org.junit.Test;

public class GenerateSeriesTest {

  private GenerateSeries rangeUdf = new GenerateSeries();

  @Test
  public void shouldComputePositiveIntRange() {
    List<Integer> range = rangeUdf.generateSeriesInt(0, 9);
    assertThat(range, hasSize(10));
    int val = 0;
    for (Integer i : range) {
      assertThat(val++, is(i));
    }
  }

  @Test
  public void shouldComputeNegativeIntRange() {
    List<Integer> range = rangeUdf.generateSeriesInt(9, 0);
    assertThat(range, hasSize(10));
    int val = 9;
    for (Integer i : range) {
      assertThat(val--, is(i));
    }
  }

  @Test
  public void shouldComputeLongRange() {
    List<Long> range = rangeUdf.generateSeriesLong(0, 9);
    assertThat(range, hasSize(10));
    long val = 0;
    for (Long i : range) {
      assertThat(val++, is(i));
    }
  }

  @Test
  public void shouldComputeNegativeLongRange() {
    List<Long> range = rangeUdf.generateSeriesLong(9, 0);
    assertThat(range, hasSize(10));
    long val = 9;
    for (Long i : range) {
      assertThat(val--, is(i));
    }
  }

  @Test
  public void shouldComputeIntRangeWithPositiveEvenStepInt() {
    List<Integer> range = rangeUdf.generateSeriesInt(0, 9, 2);
    assertThat(range, hasSize(5));
    int val = 0;
    for (int i : range) {
      assertThat(val, is(i));
      val += 2;
    }
  }

  @Test
  public void shouldComputeIntRangeWithPositiveOddStepInt() {
    List<Integer> range = rangeUdf.generateSeriesInt(0, 9, 3);
    assertThat(range, hasSize(4));
    int val = 0;
    for (int i : range) {
      assertThat(val, is(i));
      val += 3;
    }
  }

  @Test
  public void shouldComputeIntRangeWithNegativeEvenStepInt() {
    List<Integer> range = rangeUdf.generateSeriesInt(9, 0, -2);
    assertThat(range, hasSize(5));
    int val = 9;
    for (int i : range) {
      assertThat(val, is(i));
      val -= 2;
    }
  }

  @Test
  public void shouldComputeIntRangeWithNegativeOddStepInt() {
    List<Integer> range = rangeUdf.generateSeriesInt(9, 0, -3);
    assertThat(range, hasSize(4));
    int val = 9;
    for (int i : range) {
      assertThat(val, is(i));
      val -= 3;
    }
  }

  @Test
  public void shouldComputeIntRangeWithEvenStepLong() {
    List<Long> range = rangeUdf.generateSeriesLong(0, 9, 2);
    assertThat(range, hasSize(5));
    long index = 0;
    for (long i : range) {
      assertThat(index, is(i));
      index += 2;
    }
  }

  @Test
  public void shouldComputeIntRangeWithOddStepLong() {
    List<Long> range = rangeUdf.generateSeriesLong(0, 9, 3);
    assertThat(range, hasSize(4));
    long index = 0;
    for (long i : range) {
      assertThat(index, is(i));
      index += 3;
    }
  }

  @Test
  public void shouldComputeIntRangeWithNegativeEvenStepLong() {
    List<Long> range = rangeUdf.generateSeriesLong(9, 0, -2);
    assertThat(range, hasSize(5));
    long val = 9;
    for (long i : range) {
      assertThat(val, is(i));
      val -= 2;
    }
  }

  @Test
  public void shouldComputeIntRangeWithNegativeOddStepLong() {
    List<Long> range = rangeUdf.generateSeriesLong(9, 0, -3);
    assertThat(range, hasSize(4));
    long val = 9;
    for (long i : range) {
      assertThat(val, is(i));
      val -= 3;
    }
  }

  @Test
  public void shouldThrowOnStepZeroInt() {
    // When:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> rangeUdf.generateSeriesInt(0, 10, 0)
    );

    // Then:
    assertThat(e.getMessage(), containsString("GENERATE_SERIES step cannot be zero"));
  }

  @Test
  public void shouldThrowOnStepZeroLong() {
    // When:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> rangeUdf.generateSeriesLong(0L, 10L, 0)
    );

    // Then:
    assertThat(e.getMessage(), containsString("GENERATE_SERIES step cannot be zero"));
  }

  @Test
  public void shouldThrowIfStepWrongSignInt1() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> rangeUdf.generateSeriesInt(0, 10, -1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("GENERATE_SERIES step has wrong sign"));
  }

  @Test
  public void shouldThrowIfStepWrongSignInt2() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> rangeUdf.generateSeriesInt(9, 0, 1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("GENERATE_SERIES step has wrong sign"));
  }

  @Test
  public void shouldThrowIfStepWrongSignLong1() {
    // When:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> rangeUdf.generateSeriesLong(0, 10, -1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("GENERATE_SERIES step has wrong sign"));
  }

  @Test
  public void shouldThrowIfStepWrongSignLong2() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> rangeUdf.generateSeriesLong(9, 0, 1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("GENERATE_SERIES step has wrong sign"));
  }

}
