/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.count;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.primitives.Ints;
import io.confluent.ksql.function.udaf.Udaf;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class CountDistinctKudafTest {

  @Test
  public void shouldCountStrings() {
    // Given:
    final Udaf<String, List<Integer>, Long> udaf = CountDistinct.distinct();
    final String[] values = IntStream
        .range(0, 100)
        .mapToObj(i -> String.valueOf(i % 4))
        .toArray(String[]::new);

    List<Integer> agg = udaf.initialize();

    // When:
    for (final String value : values) {
      agg = udaf.aggregate(value, agg);
    }

    // Then:
    assertThat(udaf.map(agg), is(4L));
  }

  @Test
  public void shouldCountList() {
    // Given:
    final Udaf<List<Integer>, List<Integer>, Long> udaf = CountDistinct.distinct();
    final List<List<Integer>> values = IntStream
        .range(0, 100)
        .mapToObj(i -> Ints.asList(i % 4))
        .collect(Collectors.toList());

    List<Integer> agg = udaf.initialize();

    // When:
    for (final List<Integer> value : values) {
      agg = udaf.aggregate(value, agg);
    }

    // Then:
    assertThat(udaf.map(agg), is(4L));
  }

  @Test
  public void shouldIgnoreNulls() {
    // Given:
    final Udaf<String, List<Integer>, Long> udaf = CountDistinct.distinct();
    List<Integer> agg = udaf.initialize();

    // When:
    agg = udaf.aggregate(null, agg);

    // Then:
    assertThat(udaf.map(agg), is(0L));
  }

  @Test
  public void shouldMerge() {
    // Given:
    final Udaf<String, List<Integer>, Long> udaf = CountDistinct.distinct();
    final String[] values1 = IntStream
        .range(0, 100)
        .mapToObj(i -> String.valueOf(i % 4))
        .toArray(String[]::new);

    List<Integer> agg1 = udaf.initialize();
    List<Integer> agg2 = udaf.initialize();

    // When:
    for (final String value : values1) {
      agg1 = udaf.aggregate(value, agg1);
    }

    for (final String value : new String[]{"5"}) {
      agg2 = udaf.aggregate(value, agg2);
    }

    // Then:
    assertThat(udaf.map(udaf.merge(agg1, agg2)), is(5L));
  }

}