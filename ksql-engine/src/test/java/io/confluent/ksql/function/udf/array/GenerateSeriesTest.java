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

  private GenerateSeries rangeUdf = new GenerateSeries();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldComputeIntRange() {
    List<Integer> range = rangeUdf.generateSeriesInt(0, 9);
    assertThat(range, hasSize(10));
    int index = 0;
    for (Integer i : range) {
      assertThat(index++, is(i));
    }
  }

  @Test
  public void shouldComputeEmptyIntRange() {
    List<Integer> range = rangeUdf.generateSeriesInt(5, 4);
    assertThat(range, hasSize(0));
  }

  @Test
  public void shouldComputeEmptyIntRangeWhenEndLessThanStart() {
    List<Integer> range = rangeUdf.generateSeriesInt(5, 0);
    assertThat(range, hasSize(0));
  }

  @Test
  public void shouldComputeLongRange() {
    List<Long> range = rangeUdf.generateSeriesLong(0, 9);
    assertThat(range, hasSize(10));
    long index = 0;
    for (Long i : range) {
      assertThat(index++, is(i));
    }
  }

  @Test
  public void shouldComputeEmptyLongRange() {
    List<Long> range = rangeUdf.generateSeriesLong(5, 4);
    assertThat(range, hasSize(0));
  }

  @Test
  public void shouldComputeEmptyLongRangeWhenEndLessThanStart() {
    List<Long> range = rangeUdf.generateSeriesLong(5, 0);
    assertThat(range, hasSize(0));
  }

  @Test
  public void shouldComputeIntRangeWithEvenStepInt() {
    List<Integer> range = rangeUdf.generateSeriesInt(0, 9, 2);
    assertThat(range, hasSize(5));
    int index = 0;
    for (int i : range) {
      assertThat(index, is(i));
      index += 2;
    }
  }

  @Test
  public void shouldComputeIntRangeWithOddStepInt() {
    List<Integer> range = rangeUdf.generateSeriesInt(0, 9, 3);
    assertThat(range, hasSize(4));
    int index = 0;
    for (int i : range) {
      assertThat(index, is(i));
      index += 3;
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
  public void shouldThrowOnStepZeroInt() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GENERATE_SERIES step must be > 0");
    rangeUdf.generateSeriesInt(0, 10, 0);
  }

  @Test
  public void shouldThrowOnStepNegativeInt() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GENERATE_SERIES step must be > 0");
    rangeUdf.generateSeriesInt(0, 10, -1);
  }

  @Test
  public void shouldThrowOnStepZeroLong() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GENERATE_SERIES step must be > 0");
    rangeUdf.generateSeriesLong(0L, 10L, 0);
  }

  @Test
  public void shouldThrowOnStepNegativeLong() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("GENERATE_SERIES step must be > 0");
    rangeUdf.generateSeriesLong(0L, 10L, -1);
  }

}
