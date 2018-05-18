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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class IntTopkDistinctKudafTest {

  private ArrayList<Integer> valueArray;
  private final TopkDistinctKudaf<Integer> intTopkDistinctKudaf =
      TopKDistinctTestUtils.getTopKDistinctKudaf(3, Schema.INT32_SCHEMA);

  @Before
  public void setup() {
    valueArray = new ArrayList<Integer>(Arrays.asList(10, 30, 45, 10, 50, 60, 20, 60, 80, 35, 25,
                                                      60, 80));
  }

  @Test
  public void shouldAggregateTopK() {
    ArrayList<Integer> currentVal = new ArrayList<Integer>();
    for (Integer d : valueArray) {
      currentVal = intTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(new ArrayList<Integer>(Arrays.asList(80, 60, 50))));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    ArrayList<Integer> currentVal = new ArrayList<Integer>();
    currentVal = intTopkDistinctKudaf.aggregate(80, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(new ArrayList<Integer>(Arrays.asList(80))));
  }

  @Test
  public void shouldMergeTopK() {
    ArrayList<Integer> array1 = new ArrayList<Integer>(Arrays.asList(50, 45, 25));
    ArrayList<Integer> array2 = new ArrayList<Integer>(Arrays.asList(60, 50, 48));

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2),
               equalTo(
                   new ArrayList<Integer>(Arrays.asList(60, 50, 48))));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    ArrayList<Integer> array1 = new ArrayList<Integer>(Arrays.asList(50, 45));
    ArrayList<Integer> array2 = new ArrayList<Integer>(Arrays.asList(60));

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2),
               equalTo(
                   new ArrayList<Integer>(Arrays.asList(60, 50, 45))));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    ArrayList<Integer> array1 = new ArrayList<Integer>(Arrays.asList(50, 45));
    ArrayList<Integer> array2 = new ArrayList<Integer>(Arrays.asList(60, 50));

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2),
               equalTo(
                   new ArrayList<Integer>(Arrays.asList(60, 50, 45))));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    ArrayList<Integer> array1 = new ArrayList<Integer>(Arrays.asList(60));
    ArrayList<Integer> array2 = new ArrayList<Integer>(Arrays.asList(60));

    assertThat("Invalid results.", intTopkDistinctKudaf.getMerger().apply("key", array1, array2),
               equalTo(
                   new ArrayList<Integer>(Arrays.asList(60))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldAggregateAndProducedOrderedTopK() {
    ArrayList<Integer> aggregate = intTopkDistinctKudaf.aggregate(1, new ArrayList<Integer>());
    assertThat(aggregate, equalTo(new ArrayList<Integer>(Arrays.asList(1))));
    ArrayList<Integer> agg2 = intTopkDistinctKudaf.aggregate(100, new ArrayList<Integer>(Arrays
                                                                                             .asList
                                                                                                 (1)));
    assertThat(agg2, equalTo(new ArrayList<Integer>(Arrays.asList(100, 1))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldBeThreadSafe() {
    // Given:
    final TopkDistinctKudaf<Integer> intTopkDistinctKudaf =
        TopKDistinctTestUtils.getTopKDistinctKudaf(12, Schema.INT32_SCHEMA);

    final List<Integer> values = ImmutableList.of(10, 30, 45, 10, 50, 60, 20, 70, 80, 35, 25);

    // When:
    final ArrayList<Integer> result = IntStream.range(0, 4)
        .parallel()
        .mapToObj(threadNum -> {
          ArrayList<Integer> aggregate = new ArrayList<Integer>(Arrays.asList(1, 1, 1, 1, 1, 1,
                                                                              1, 1, 1, 1, 1, 1));

          for (int value : values) {
            aggregate = intTopkDistinctKudaf.aggregate(value + threadNum, aggregate);
          }
          return aggregate;
        })
        .reduce((agg1, agg2) -> intTopkDistinctKudaf.getMerger().apply("blah", agg1, agg2))
        .orElse(new ArrayList<Integer>());

    // Then:
    assertThat(result, is(Arrays.asList(83, 82, 81, 80, 73, 72, 71, 70, 63, 62, 61, 60)));
  }

  @SuppressWarnings("unchecked")
  //@Test
  public void testAggregatePerformance() {
    final int iterations = 1_000_000_000;
    final int topX = 10;
    final TopkDistinctKudaf<Integer> intTopkDistinctKudaf =
        new TopkDistinctKudaf("TopkDistinctKudaf", 0, topX, Schema.INT32_SCHEMA, Integer.class);
    final List<Integer> aggregate = IntStream.range(0, topX)
        .mapToObj(Integer::valueOf)
        .collect(Collectors.toList());
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      intTopkDistinctKudaf.aggregate(i, new ArrayList<>(aggregate));
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
        TopKDistinctTestUtils.getTopKDistinctKudaf(topX, Schema.INT32_SCHEMA);

    final List<Integer> aggregate1 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v + 1 : v)
        .collect(Collectors.toList());
    final List<Integer> aggregate2 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v : v + 1)
        .collect(Collectors.toList());
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      intTopkDistinctKudaf.getMerger().apply("ignored", new ArrayList<>(aggregate1), new
          ArrayList<>(aggregate2));
    }

    final long took = System.currentTimeMillis() - start;
    System.out.println(took + "ms, " + ((double)took)/iterations);
  }
}