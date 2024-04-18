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

package io.confluent.ksql.function.udaf.topk;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class IntTopkKudafTest {

  private final List<Integer> valuesArray = ImmutableList.of(10, 30, 45, 10, 50, 60, 20, 60, 80, 35, 25);
  private Udaf<Integer, List<Integer>, List<Integer>> topkKudaf;

  @Before
  public void setup() {
    topkKudaf = TopkKudaf.createTopKInt(3);
    topkKudaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)));
  }

  @Test
  public void shouldAggregateTopK() {
    List<Integer> currentVal = new ArrayList<>();
    for (final Integer value : valuesArray) {
      currentVal = topkKudaf.aggregate(value, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(80, 60, 60)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    List<Integer> currentVal = new ArrayList<>();
    currentVal = topkKudaf.aggregate(10, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(10)));
  }

  @Test
  public void shouldMergeTopK() {
    final List<Integer> array1 = ImmutableList.of(50, 45, 25);
    final List<Integer> array2 = ImmutableList.of(60, 55, 48);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60, 55, 50)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final List<Integer> array1 = ImmutableList.of(50, 45);
    final List<Integer> array2 = ImmutableList.of(60);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60, 50, 45)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final List<Integer> array1 = ImmutableList.of(50);
    final List<Integer> array2 = ImmutableList.of(60);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60, 50)));
  }

  @Test
  public void shouldAggregateAndProducedOrderedTopK() {
    final List<Integer> aggregate = topkKudaf.aggregate(1, new ArrayList<>());
    assertThat(aggregate, equalTo(ImmutableList.of(1)));
    final List<Integer> agg2 = topkKudaf.aggregate(100, aggregate);
    assertThat(agg2, equalTo(ImmutableList.of(100, 1)));
  }

  @Test
  public void shouldWorkWithLargeValuesOfKay() {
    // Given:
    final int topKSize = 300;
    topkKudaf = createUdaf(topKSize);
    final List<Integer> initialAggregate = IntStream.range(0, topKSize)
            .boxed().sorted(Comparator.reverseOrder()).collect(Collectors.toList());

    // When:
    final List<Integer> result = topkKudaf.aggregate(10, initialAggregate);
    final List<Integer> combined = topkKudaf.merge(result, initialAggregate);

    // Then:
    assertThat(combined.get(0), is(299));
    assertThat(combined.get(1), is(299));
    assertThat(combined.get(2), is(298));
  }

  @Test
  public void shouldBeThreadSafe() {
    // Given:
    topkKudaf = createUdaf(12);

    final List<Integer> values = ImmutableList.of(10, 30, 45, 10, 50, 60, 20, 70, 80, 35, 25);

    // When:
    final List<Integer> result = IntStream.range(0, 4)
        .parallel()
        .mapToObj(threadNum -> {
          List<Integer> aggregate = Arrays.asList(0, 0, 0, 0, 0, 0, 0, 0, 0,
              0, 0, 0);
          for (int value : values) {
            aggregate = topkKudaf.aggregate(value + threadNum, aggregate);
          }
          return aggregate;
        })
        .reduce((agg1, agg2) -> topkKudaf.merge(agg1, agg2))
        .orElse(new ArrayList<>());

    // Then:
    assertThat(result, is(ImmutableList.of(83, 82, 81, 80, 73, 72, 71, 70, 63, 62, 61, 60)));
  }

  //@Test
  public void testAggregatePerformance() {
    final int iterations = 1_000_000_000;
    final int topX = 10;
    topkKudaf = createUdaf(topX);
    final List<Integer> aggregate = new ArrayList<>();
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      topkKudaf.aggregate(i, aggregate);
    }

    final long took = System.currentTimeMillis() - start;
    System.out.println(took + "ms, " + ((double)took)/iterations);
  }

  //@Test
  public void testMergePerformance() {
    final int iterations = 1_000_000_000;
    final int topX = 10;
    topkKudaf = createUdaf(topX);

    final List<Integer> aggregate1 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v + 1 : v)
        .collect(Collectors.toList());
    final List<Integer> aggregate2 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v : v + 1)
        .collect(Collectors.toList());
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      topkKudaf.merge(aggregate1, aggregate2);
    }

    final long took = System.currentTimeMillis() - start;
    System.out.println(took + "ms, " + ((double)took)/iterations);
  }

  private Udaf<Integer, List<Integer>, List<Integer>> createUdaf(final int k) {
    Udaf<Integer, List<Integer>, List<Integer>> udaf = TopkKudaf.createTopKInt(k);
    udaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER)));
    return udaf;
  }
}