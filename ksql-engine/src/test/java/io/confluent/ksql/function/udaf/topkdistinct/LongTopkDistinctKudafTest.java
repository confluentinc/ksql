/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

public class LongTopkDistinctKudafTest {

  private final List<Long> valuesArray = ImmutableList.of(10L, 30L, 45L, 10L, 50L, 60L, 20L, 60L, 80L, 35L, 25L,
      60L, 80L);
  private final TopkDistinctKudaf<Long> longTopkDistinctKudaf
      = TopKDistinctTestUtils.getTopKDistinctKudaf(3, Schema.OPTIONAL_INT64_SCHEMA);

  @Test
  public void shouldAggregateTopK() {
    List<Long> currentVal = new ArrayList<>();
    for (final Long d: valuesArray) {
      currentVal = longTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(80L, 60L, 50L)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    List<Long> currentVal = new ArrayList<>();
    currentVal = longTopkDistinctKudaf.aggregate(80L, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(80L)));
  }

  @Test
  public void shouldMergeTopK() {
    final List<Long> array1 = ImmutableList.of(50L, 45L, 25L);
    final List<Long> array2 = ImmutableList.of(60L, 50L, 48L);

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60L, 50L, 48L)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final List<Long> array1 = ImmutableList.of(50L, 45L);
    final List<Long> array2 = ImmutableList.of(60L);

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60L, 50L, 45L)));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    final List<Long> array1 = ImmutableList.of(50L, 45L);
    final List<Long> array2 = ImmutableList.of(60L, 50L);

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60L, 50L, 45L)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final List<Long> array1 = ImmutableList.of(60L);
    final List<Long> array2 = ImmutableList.of(60L);

    assertThat("Invalid results.", longTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        ImmutableList.of(60L)));
  }
}