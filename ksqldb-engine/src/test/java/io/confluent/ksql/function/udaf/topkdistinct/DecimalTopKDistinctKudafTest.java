/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.function.udaf.topkdistinct;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class DecimalTopKDistinctKudafTest {

  private final List<BigDecimal> valuesArray = toDecimal(ImmutableList.of(10.0, 30.0, 45.0, 10.0, 50.0, 60.0, 20.0, 60.0,
      80.0, 35.0, 25.0, 60.0, 80.0));
  private final TopkDistinctKudaf<BigDecimal> doubleTopkDistinctKudaf
      = TopKDistinctTestUtils.getTopKDistinctKudaf(3, SqlTypes.DOUBLE);

  @Test
  public void shouldAggregateTopK() {
    List<BigDecimal> currentVal = new ArrayList<>();
    for (final BigDecimal d: valuesArray) {
      currentVal = doubleTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(toDecimal(ImmutableList.of(80.0, 60.0, 50.0))));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    List<BigDecimal> currentVal = new ArrayList<>();
    currentVal = doubleTopkDistinctKudaf.aggregate(new BigDecimal("80.0"), currentVal);

    assertThat("Invalid results.", currentVal, equalTo(toDecimal(ImmutableList.of(80.0))));
  }

  @Test
  public void shouldMergeTopK() {
    final List<BigDecimal> array1 = toDecimal(ImmutableList.of(50.0, 45.0, 25.0));
    final List<BigDecimal> array2 = toDecimal(ImmutableList.of(60.0, 50.0, 48.0));

    assertThat("Invalid results.", doubleTopkDistinctKudaf.merge(array1, array2), equalTo(
        toDecimal(ImmutableList.of(60.0, 50.0, 48.0))));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final List<BigDecimal> array1 = toDecimal(ImmutableList.of(50.0, 45.0));
    final List<BigDecimal> array2 = toDecimal(ImmutableList.of(60.0));

    assertThat("Invalid results.", doubleTopkDistinctKudaf.merge(array1, array2), equalTo(
        toDecimal(ImmutableList.of(60.0, 50.0, 45.0))));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    final List<BigDecimal> array1 = toDecimal(ImmutableList.of(50.0, 45.0));
    final List<BigDecimal> array2 = toDecimal(ImmutableList.of(60.0, 50.0));

    assertThat("Invalid results.", doubleTopkDistinctKudaf.merge(array1, array2), equalTo(
        toDecimal(ImmutableList.of(60.0, 50.0, 45.0))));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final List<BigDecimal> array1 = toDecimal(ImmutableList.of(60.0));
    final List<BigDecimal> array2 = toDecimal(ImmutableList.of(60.0));

    assertThat("Invalid results.", doubleTopkDistinctKudaf.merge(array1, array2), equalTo(
        toDecimal(ImmutableList.of(60.0))));
  }

  private static List<BigDecimal> toDecimal(final List<Double> doubles) {
    return doubles.stream().map(String::valueOf).map(BigDecimal::new).collect(Collectors.toList());
  }

}
