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

package io.confluent.ksql.function.udaf.topk;

import com.google.common.collect.ImmutableList;

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.confluent.ksql.function.KsqlAggregateFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class IntTopkKudafTest {

  private ArrayList<Integer> valueArray;
  private KsqlAggregateFunction<Integer, ArrayList<Integer>> topkKudaf;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    valueArray = new ArrayList(Arrays.asList(10, 30, 45, 10, 50, 60, 20, 60, 80, 35, 25));
    topkKudaf = new TopKAggregateFunctionFactory(3)
        .getProperAggregateFunction(Collections.singletonList(Schema.INT32_SCHEMA));
  }

  @Test
  public void shouldAggregateTopK() {
    ArrayList<Integer> currentVal = new ArrayList();
    for (Integer value : valueArray) {
      currentVal = topkKudaf.aggregate(value, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(Arrays.asList(80, 60, 60)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    ArrayList<Integer> currentVal = new ArrayList();
    currentVal = topkKudaf.aggregate(10, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(Arrays.asList(10)));
  }

  @Test
  public void shouldMergeTopK() {
    ArrayList<Integer> array1 = new ArrayList(Arrays.asList(50, 45, 25));
    ArrayList<Integer> array2 = new ArrayList(Arrays.asList(60, 55, 48));

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
               equalTo(Arrays.asList(60, 55, 50)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    ArrayList<Integer> array1 = new ArrayList(Arrays.asList(50, 45));
    ArrayList<Integer> array2 = new ArrayList(Arrays.asList(60));

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
               equalTo(Arrays.asList(60, 50, 45)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    ArrayList<Integer> array1 = new ArrayList(Arrays.asList(50));
    ArrayList<Integer> array2 = new ArrayList(Arrays.asList(60));

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
               equalTo(Arrays.asList(60, 50)));
  }

  @Test
  public void shouldAggregateAndProducedOrderedTopK() {
    ArrayList aggregate = topkKudaf.aggregate(1, new ArrayList());
    assertThat(aggregate, equalTo(Arrays.asList(1)));
    ArrayList agg2 = topkKudaf.aggregate(100, new ArrayList(Arrays.asList(1)));
    assertThat(agg2, equalTo(Arrays.asList(100, 1)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldWorkWithLargeValuesOfKay() {
    // Given:
    final int topKSize = 300;
    topkKudaf = new TopKAggregateFunctionFactory(topKSize)
        .getProperAggregateFunction(Collections.singletonList(Schema.INT32_SCHEMA));
    final List<Integer> initialAggregate = IntStream.range(0, topKSize)
        .mapToObj(Integer::valueOf)
        .collect(Collectors.toList());

    // When:
    final ArrayList<Integer> result = topkKudaf.aggregate(10, new ArrayList<>(initialAggregate));
    final ArrayList<Integer> combined = topkKudaf.getMerger().apply("key", result, new ArrayList<>(initialAggregate));

    // Then:
    assertThat(combined.get(0), is(299));
    assertThat(combined.get(1), is(299));
    assertThat(combined.get(2), is(298));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldBeThreadSafe() {
    // Given:
    topkKudaf = new TopKAggregateFunctionFactory(12)
        .getProperAggregateFunction(Collections.singletonList(Schema.INT32_SCHEMA));

    final List<Integer> values = ImmutableList.of(10, 30, 45, 10, 50, 60, 20, 70, 80, 35, 25);

    // When:
    final ArrayList result = IntStream.range(0, 4)
        .parallel()
        .mapToObj(threadNum -> {
          ArrayList<Integer> aggregate = new ArrayList(Arrays.asList(0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                     0, 0, 0));
          for (int value : values) {
            aggregate = topkKudaf.aggregate(value + threadNum, aggregate);
          }
          return aggregate;
        })
        .reduce((agg1, agg2) -> topkKudaf.getMerger().apply("blah", agg1, agg2))
        .orElse(new ArrayList<>());

    // Then:
    assertThat(result, is(Arrays.asList(83, 82, 81, 80, 73, 72, 71, 70, 63, 62, 61, 60)));
  }

  @SuppressWarnings("unchecked")
  //@Test
  public void testAggregatePerformance() {
    final int iterations = 1_000_000_000;
    final int topX = 10;
    topkKudaf = new TopKAggregateFunctionFactory(topX)
        .getProperAggregateFunction(Collections.singletonList(Schema.INT32_SCHEMA));
    final List<Integer> aggregate = IntStream.range(0, topX)
        .mapToObj(Integer::valueOf)
        .collect(Collectors.toList());
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      topkKudaf.aggregate(i, new ArrayList<>(aggregate));
    }

    final long took = System.currentTimeMillis() - start;
    System.out.println(took + "ms, " + ((double)took)/iterations);
  }

  @SuppressWarnings("unchecked")
  //@Test
  public void testMergePerformance() {
    final int iterations = 1_000_000_000;
    final int topX = 10;
    topkKudaf = new TopKAggregateFunctionFactory(topX)
        .getProperAggregateFunction(Collections.singletonList(Schema.INT32_SCHEMA));

    final List<Integer> aggregate1 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v + 1 : v)
        .collect(Collectors.toList());
    final List<Integer> aggregate2 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v : v + 1)
        .collect(Collectors.toList());
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      topkKudaf.getMerger().apply("ignmored", new ArrayList<>(aggregate1), new ArrayList<>
          (aggregate2));
    }

    final long took = System.currentTimeMillis() - start;
    System.out.println(took + "ms, " + ((double)took)/iterations);
  }
}