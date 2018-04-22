/*
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

import com.google.common.collect.ImmutableList;

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class IntTopkDistinctKudafTest {

  private Integer[] valueArray;
  private final TopkDistinctKudaf<Integer> intTopkDistinctKudaf =
      new TopkDistinctKudaf<>(0, 3, Schema.INT32_SCHEMA, Integer.class);

  @Before
  public void setup() {
    valueArray = new Integer[]{10, 30, 45, 10, 50, 60, 20, 60, 80, 35, 25,
                               60, 80};
  }

  @Test
  public void shouldAggregateTopK() {
    Integer[] currentVal = new Integer[]{null, null, null};
    for (Integer d : valueArray) {
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

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2),
               equalTo(
                   new Integer[]{60, 50, 48}));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    Integer[] array1 = new Integer[]{50, 45, null};
    Integer[] array2 = new Integer[]{60, null, null};

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2),
               equalTo(
                   new Integer[]{60, 50, 45}));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    Integer[] array1 = new Integer[]{50, 45, null};
    Integer[] array2 = new Integer[]{60, 50, null};

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2),
               equalTo(
                   new Integer[]{60, 50, 45}));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    Integer[] array1 = new Integer[]{60, null, null};
    Integer[] array2 = new Integer[]{60, null, null};

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2),
               equalTo(
                   new Integer[]{60, null, null}));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldAggregateAndProducedOrderedTopK() {
    Object[] aggregate = intTopkDistinctKudaf.aggregate(1, new Integer[3]);
    assertThat(aggregate, equalTo(new Integer[]{1, null, null}));
    Object[] agg2 = intTopkDistinctKudaf.aggregate(100, new Integer[]{1, null, null});
    assertThat(agg2, equalTo(new Integer[]{100, 1, null}));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldBeThreadSafe() {
    // Given:
    final TopkDistinctKudaf<Integer> intTopkDistinctKudaf =
        new TopkDistinctKudaf<>(0, 12, Schema.INT32_SCHEMA, Integer.class);

    final List<Integer> values = ImmutableList.of(10, 30, 45, 10, 50, 60, 20, 70, 80, 35, 25);

    // When:
    final Object[] result = IntStream.range(0, 4)
        .parallel()
        .mapToObj(threadNum -> {
          Integer[] aggregate = new Integer[]
              {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};

          for (int value : values) {
            aggregate = intTopkDistinctKudaf.aggregate(value + threadNum, aggregate);
          }
          return aggregate;
        })
        .reduce((agg1, agg2) -> intTopkDistinctKudaf.getMerger().apply("blah", agg1, agg2))
        .orElse(new Integer[0]);

    // Then:
    assertThat(result, is(new Object[]{83, 82, 81, 80, 73, 72, 71, 70, 63, 62, 61, 60}));
  }

  @SuppressWarnings("unchecked")
  //@Test
  public void testAggregatePerformance() {
    final int iterations = 1_000_000_000;
    final int topX = 10;
    final TopkDistinctKudaf<Integer> intTopkDistinctKudaf =
        new TopkDistinctKudaf<>(0, topX, Schema.INT32_SCHEMA, Integer.class);
    final Integer[] aggregate = IntStream.range(0, topX)
        .mapToObj(idx -> null)
        .toArray(Integer[]::new);
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      intTopkDistinctKudaf.aggregate(i, aggregate);
    }

    final long took = System.currentTimeMillis() - start;
    System.out.println(took + "ms, " + ((double)took)/iterations);
  }

  @SuppressWarnings("unchecked")
  //@Test
  public void testMergePerformance() {
    final int iterations = 1_000_000_000;
    final int topX = 10;
    final TopkDistinctKudaf<Integer> intTopkDistinctKudaf =
        new TopkDistinctKudaf<>(0, topX, Schema.INT32_SCHEMA, Integer.class);

    final Integer[] aggregate1 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v + 1 : v)
        .toArray(Integer[]::new);
    final Integer[] aggregate2 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v : v + 1)
        .toArray(Integer[]::new);
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      intTopkDistinctKudaf.getMerger().apply("ignored", aggregate1, aggregate2);
    }

    final long took = System.currentTimeMillis() - start;
    System.out.println(took + "ms, " + ((double)took)/iterations);
  }
}
