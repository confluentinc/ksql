/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udaf.topkdistinct;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class DoubleTopkDistinctKudafTest {

  private final List<Double> valuesArray = ImmutableList.of(10.0, 30.0, 45.0, 10.0, 50.0, 60.0, 20.0, 60.0,
      80.0, 35.0, 25.0, 60.0, 80.0);
  private final TopkDistinctKudaf<Double> doubleTopkDistinctKudaf
      = TopKDistinctTestUtils.getTopKDistinctKudaf(3, SqlTypes.DOUBLE);

  @Test
  public void shouldAggregateTopK() {
    List<Double> currentVal = new ArrayList<>();
    for (final Double d: valuesArray) {
      currentVal = doubleTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(80.0, 60.0, 50.0)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    List<Double> currentVal = new ArrayList<>();
    currentVal = doubleTopkDistinctKudaf.aggregate(80.0, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(80.0)));
  }

  @Test
  public void shouldMergeTopK() {
    final List<Double> array1 = ImmutableList.of(50.0, 45.0, 25.0);
    final List<Double> array2 = ImmutableList.of(60.0, 50.0, 48.0);

    assertThat("Invalid results.", doubleTopkDistinctKudaf.merge(array1, array2), equalTo(
        ImmutableList.of(60.0, 50.0, 48.0)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final List<Double> array1 = ImmutableList.of(50.0, 45.0);
    final List<Double> array2 = ImmutableList.of(60.0);

    assertThat("Invalid results.", doubleTopkDistinctKudaf.merge(array1, array2), equalTo(
        ImmutableList.of(60.0, 50.0, 45.0)));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    final List<Double> array1 = ImmutableList.of(50.0, 45.0);
    final List<Double> array2 = ImmutableList.of(60.0, 50.0);

    assertThat("Invalid results.", doubleTopkDistinctKudaf.merge(array1, array2), equalTo(
        ImmutableList.of(60.0, 50.0, 45.0)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final List<Double> array1 = ImmutableList.of(60.0);
    final List<Double> array2 = ImmutableList.of(60.0);

    assertThat("Invalid results.", doubleTopkDistinctKudaf.merge(array1, array2), equalTo(
        ImmutableList.of(60.0)));
  }
}