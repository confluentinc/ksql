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

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class LongTopkDistinctKudafTest {

  ArrayList<Long> valueArray;
  private final TopkDistinctKudaf<Long> longTopkDistinctKudaf
      = TopKDistinctTestUtils.getTopKDistinctKudaf(3, Schema.INT64_SCHEMA);

  @Before
  public void setup() {
    valueArray = new ArrayList(Arrays.asList(10L, 30L, 45L, 10L, 50L, 60L, 20L, 60L, 80L, 35L, 25L,
                                             60L, 80L));

  }

  @Test
  public void shouldAggregateTopK() {
    ArrayList<Long> currentVal = new ArrayList();
    for (Long d: valueArray) {
      currentVal = longTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(new ArrayList(Arrays.asList(80L, 60L,
                                                                                   50L))));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    ArrayList<Long> currentVal = new ArrayList();
    currentVal = longTopkDistinctKudaf.aggregate(80L, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(new ArrayList(Arrays.asList(80L))));
  }

  @Test
  public void shouldMergeTopK() {
    ArrayList<Long> array1 = new ArrayList(Arrays.asList(50L, 45L, 25L));
    ArrayList<Long> array2 = new ArrayList(Arrays.asList(60L, 50L, 48l));

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new ArrayList(Arrays.asList(60L, 50L, 48l))));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    ArrayList<Long> array1 = new ArrayList(Arrays.asList(50L, 45L));
    ArrayList<Long> array2 = new ArrayList(Arrays.asList(60L));

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new ArrayList(Arrays.asList(60L, 50L, 45L))));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    ArrayList<Long> array1 = new ArrayList(Arrays.asList(50L, 45L));
    ArrayList<Long> array2 = new ArrayList(Arrays.asList(60L, 50L));

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new ArrayList(Arrays.asList(60L, 50L, 45L))));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    ArrayList<Long> array1 = new ArrayList(Arrays.asList(60L));
    ArrayList<Long> array2 = new ArrayList(Arrays.asList(60L));

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new ArrayList(Arrays.asList(60L))));
  }
}