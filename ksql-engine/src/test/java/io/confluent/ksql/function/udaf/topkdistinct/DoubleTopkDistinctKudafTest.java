/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udaf.topkdistinct;

import com.google.common.collect.ImmutableList;

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class DoubleTopkDistinctKudafTest {

  private final List<Double> valuesArray = ImmutableList.of(10.0, 30.0, 45.0, 10.0, 50.0, 60.0, 20.0, 60.0,
      80.0, 35.0, 25.0, 60.0, 80.0);;
  private final TopkDistinctKudaf<Double> doubleTopkDistinctKudaf
      = TopKDistinctTestUtils.getTopKDistinctKudaf(3, Schema.OPTIONAL_FLOAT64_SCHEMA);

  @Test
  public void shouldAggregateTopK() {
    List<Double> currentVal = new ArrayList<>();
    for (Double d: valuesArray) {
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
    List<Double> array1 = ImmutableList.of(50.0, 45.0, 25.0);
    List<Double> array2 = ImmutableList.of(60.0, 50.0, 48.0);

    assertThat("Invalid results.", doubleTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60.0, 50.0, 48.0)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    List<Double> array1 = ImmutableList.of(50.0, 45.0);
    List<Double> array2 = ImmutableList.of(60.0);

    assertThat("Invalid results.", doubleTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60.0, 50.0, 45.0)));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    List<Double> array1 = ImmutableList.of(50.0, 45.0);
    List<Double> array2 = ImmutableList.of(60.0, 50.0);

    assertThat("Invalid results.", doubleTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60.0, 50.0, 45.0)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    List<Double> array1 = ImmutableList.of(60.0);
    List<Double> array2 = ImmutableList.of(60.0);

    assertThat("Invalid results.", doubleTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60.0)));
  }
}