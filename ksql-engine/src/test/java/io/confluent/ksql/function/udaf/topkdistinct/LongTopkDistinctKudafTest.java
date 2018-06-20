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

public class LongTopkDistinctKudafTest {

  private final List<Long> valuesArray = ImmutableList.of(10L, 30L, 45L, 10L, 50L, 60L, 20L, 60L, 80L, 35L, 25L,
      60L, 80L);
  private final TopkDistinctKudaf<Long> longTopkDistinctKudaf
      = TopKDistinctTestUtils.getTopKDistinctKudaf(3, Schema.OPTIONAL_INT64_SCHEMA);

  @Test
  public void shouldAggregateTopK() {
    List<Long> currentVal = new ArrayList<>();
    for (Long d: valuesArray) {
      currentVal = longTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(80L, 60L, 50L)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    List<Long> currentVal = new ArrayList<>();
    currentVal = longTopkDistinctKudaf.aggregate(80L, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(80L)));
  }

  @Test
  public void shouldMergeTopK() {
    List<Long> array1 = ImmutableList.of(50L, 45L, 25L);
    List<Long> array2 = ImmutableList.of(60L, 50L, 48L);

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60L, 50L, 48L)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    List<Long> array1 = ImmutableList.of(50L, 45L);
    List<Long> array2 = ImmutableList.of(60L);

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60L, 50L, 45L)));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    List<Long> array1 = ImmutableList.of(50L, 45L);
    List<Long> array2 = ImmutableList.of(60L, 50L);

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60L, 50L, 45L)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    List<Long> array1 = ImmutableList.of(60L);
    List<Long> array2 = ImmutableList.of(60L);

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60L)));
  }
}