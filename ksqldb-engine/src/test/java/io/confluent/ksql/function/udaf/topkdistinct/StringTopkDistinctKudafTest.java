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
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class StringTopkDistinctKudafTest {

  private final List<String> valuesArray = ImmutableList.of("10", "30", "45", "10", "50", "60", "20", "60", "80", "35",
      "25", "60", "80");
  private final TopkDistinctKudaf<String> stringTopkDistinctKudaf
      = TopKDistinctTestUtils.getTopKDistinctKudaf(3, SqlTypes.STRING);

  @Test
  public void shouldAggregateTopK() {
    List<String> currentVal = new ArrayList<>();
    for (final String d: valuesArray) {
      currentVal = stringTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of("80", "60", "50")));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    List<String> currentVal = new ArrayList<>();
    currentVal = stringTopkDistinctKudaf.aggregate("80", currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of("80")));
  }

  @Test
  public void shouldMergeTopK() {
    final List<String> array1 = ImmutableList.of("50", "45", "25");
    final List<String> array2 = ImmutableList.of("60", "50", "48");

    assertThat("Invalid results.", stringTopkDistinctKudaf.merge(array1, array2), equalTo(
        ImmutableList.of("60", "50", "48")));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final List<String> array1 = ImmutableList.of("50", "45");
    final List<String> array2 = ImmutableList.of("60");

    assertThat("Invalid results.", stringTopkDistinctKudaf.merge(array1, array2), equalTo(
        ImmutableList.of("60", "50", "45")));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    final List<String> array1 = ImmutableList.of("50", "45");
    final List<String> array2 = ImmutableList.of("60", "50");

    assertThat("Invalid results.", stringTopkDistinctKudaf.merge(array1, array2), equalTo(
        ImmutableList.of("60", "50", "45")));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final List<String> array1 = ImmutableList.of("60");
    final List<String> array2 = ImmutableList.of("60");

    assertThat("Invalid results.", stringTopkDistinctKudaf.merge(array1, array2), equalTo(
        ImmutableList.of("60")));
  }
}