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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class IntTopkDistinctKudafTest {

  private final List<Integer> valuesArray =
      ImmutableList.of(10, 30, 45, 10, 50, 60, 20, 60, 80, 35, 25, 60, 80);
  private final TopkDistinctKudaf<Integer> intTopkDistinctKudaf =
      TopKDistinctTestUtils.getTopKDistinctKudaf(3, SqlTypes.INTEGER);

  @Test
  public void shouldAggregateTopK() {
    List<Integer> currentVal = new ArrayList<>();
    for (final Integer d : valuesArray) {
      currentVal = intTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(new ArrayList<>(ImmutableList.of(80,
            60, 50))));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    List<Integer> currentVal = new ArrayList<>();
    currentVal = intTopkDistinctKudaf.aggregate(80, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(80)));
  }

  @Test
  public void shouldMergeTopK() {
    final List<Integer> array1 = ImmutableList.of(50, 45, 25);
    final List<Integer> array2 = ImmutableList.of(60, 50, 48);

    assertThat("Invalid results.", intTopkDistinctKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60, 50, 48)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final List<Integer> array1 = ImmutableList.of(50, 45);
    final List<Integer> array2 = ImmutableList.of(60);

    assertThat("Invalid results.", intTopkDistinctKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60, 50, 45)));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    final List<Integer> array1 = ImmutableList.of(50, 45);
    final List<Integer> array2 = ImmutableList.of(60, 50);

    assertThat("Invalid results.", intTopkDistinctKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60, 50, 45)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final List<Integer> array1 = ImmutableList.of(60);
    final List<Integer> array2 = ImmutableList.of(60);

    assertThat("Invalid results.", intTopkDistinctKudaf.merge(array1, array2),
        equalTo(
                new ArrayList<>(ImmutableList.of(60))));
  }

  @Test
  public void shouldAggregateAndProducedOrderedTopK() {
    final List<Integer> aggregate = intTopkDistinctKudaf.aggregate(1, new ArrayList<>());
    assertThat(aggregate, equalTo(Collections.singletonList(1)));
    final List<Integer> agg2 = intTopkDistinctKudaf.aggregate(100, aggregate);
    assertThat(agg2, equalTo(ImmutableList.of(100, 1)));
  }

  @Test
  public void shouldBeThreadSafe() {
    // Given:
    final TopkDistinctKudaf<Integer> intTopkDistinctKudaf =
        TopKDistinctTestUtils.getTopKDistinctKudaf(12, SqlTypes.INTEGER);

    final List<Integer> values = ImmutableList.of(10, 30, 45, 10, 50, 60, 20, 70, 80, 35, 25);

    // When:
    final List<Integer> result = IntStream.range(0, 4)
        .parallel()
        .mapToObj(threadNum -> {
          List<Integer> aggregate = Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);

          for (int value : values) {
            aggregate = intTopkDistinctKudaf.aggregate(value + threadNum, aggregate);
          }
          return aggregate;
        })
        .reduce(intTopkDistinctKudaf::merge)
        .orElse(new ArrayList<>());

    // Then:
    assertThat(result, is(ImmutableList.of(83, 82, 81, 80, 73, 72, 71, 70, 63, 62, 61, 60)));
  }

  //@Test
  public void testAggregatePerformance() {
    final int iterations = 1_000_000_000;
    final int topX = 10;
    final TopkDistinctKudaf<Integer> intTopkDistinctKudaf =
            TopKDistinctTestUtils.getTopKDistinctKudaf(topX, SqlTypes.INTEGER);
    final List<Integer> aggregate = new ArrayList<>();
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      intTopkDistinctKudaf.aggregate(i, aggregate);
    }

    final long took = System.currentTimeMillis() - start;
    System.out.println(took + "ms, " + ((double)took)/iterations);
  }

  @SuppressFBWarnings(
          value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
          justification = "Testing performance so merge result not needed"
  )
  //@Test
  public void testMergePerformance() {
    final int iterations = 1_000_000_000;
    final int topX = 10;
    final TopkDistinctKudaf<Integer> intTopkDistinctKudaf =
        TopKDistinctTestUtils.getTopKDistinctKudaf(topX, SqlTypes.INTEGER);

    final List<Integer> aggregate1 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v + 1 : v)
        .collect(Collectors.toList());
    final List<Integer> aggregate2 = IntStream.range(0, topX)
        .mapToObj(v -> v % 2 == 0 ? v : v + 1)
        .collect(Collectors.toList());
    final long start = System.currentTimeMillis();

    for(int i = 0; i != iterations; ++i) {
      intTopkDistinctKudaf.merge(aggregate1, aggregate2);
    }

    final long took = System.currentTimeMillis() - start;
    System.out.println(took + "ms, " + ((double)took)/iterations);
  }
}