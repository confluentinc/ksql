/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udaf.topkdistinct;

import org.junit.Before;
import org.junit.Test;

import io.confluent.ksql.function.KsqlAggregateFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class IntTopkDistinctKudafTest {

  private Integer[] valueArray;
  private final TopkDistinctKudaf<Integer> intTopkDistinctKudaf = new TopkDistinctKudaf<>(0, 3, Integer.class);

  @Before
  public void setup() {
    valueArray = new Integer[]{10, 30, 45, 10, 50, 60, 20, 60, 80, 35, 25,
                              60, 80};

  }

  @Test
  public void shouldAggregateTopK() {
    Integer[] currentVal = new Integer[]{null, null, null};
    for (Integer d: valueArray) {
      currentVal = intTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(new Integer[]{80, 60, 50}));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    Integer[] currentVal = new Integer[]{null, null, null};
    currentVal = intTopkDistinctKudaf.aggregate(80, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(new Integer[]{80, null, null}));
  }

  @Test
  public void shouldMergeTopK() {
    Integer[] array1 = new Integer[]{50, 45, 25};
    Integer[] array2 = new Integer[]{60, 50, 48};

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Integer[]{60, 50, 48}));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    Integer[] array1 = new Integer[]{50, 45, null};
    Integer[] array2 = new Integer[]{60, null, null};

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Integer[]{60, 50, 45}));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    Integer[] array1 = new Integer[]{50, 45, null};
    Integer[] array2 = new Integer[]{60, 50, null};

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Integer[]{60, 50, 45}));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    Integer[] array1 = new Integer[]{60, null, null};
    Integer[] array2 = new Integer[]{60, null, null};

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Integer[]{60, null, null}));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldAggregateAndProducedOrderedTopK() {
    Object[] aggregate = intTopkDistinctKudaf.aggregate(1, new Integer[3]);
    assertThat(aggregate, equalTo(new Integer[] {1, null, null}));
    Object[] agg2 = intTopkDistinctKudaf.aggregate(100, new Integer[] {1, null, null});
    assertThat(agg2, equalTo(new Integer[] {100, 1, null}));
  }
}
