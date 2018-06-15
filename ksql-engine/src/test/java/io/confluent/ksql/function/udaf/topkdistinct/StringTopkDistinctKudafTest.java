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

import com.google.common.collect.ImmutableList;

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class StringTopkDistinctKudafTest {

  private final List<String> valuesArray = ImmutableList.of("10", "30", "45", "10", "50", "60", "20", "60", "80", "35",
      "25", "60", "80");;
  private final TopkDistinctKudaf<String> stringTopkDistinctKudaf
      = TopKDistinctTestUtils.getTopKDistinctKudaf(3, Schema.OPTIONAL_STRING_SCHEMA);

  @Test
  public void shouldAggregateTopK() {
    List<String> currentVal = new ArrayList<>();
    for (String d: valuesArray) {
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
    List<String> array1 = ImmutableList.of("50", "45", "25");
    List<String> array2 = ImmutableList.of("60", "50", "48");

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of("60", "50", "48")));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    List<String> array1 = ImmutableList.of("50", "45");
    List<String> array2 = ImmutableList.of("60");

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of("60", "50", "45")));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    List<String> array1 = ImmutableList.of("50", "45");
    List<String> array2 = ImmutableList.of("60", "50");

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of("60", "50", "45")));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    List<String> array1 = ImmutableList.of("60");
    List<String> array2 = ImmutableList.of("60");

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of("60")));
  }
}