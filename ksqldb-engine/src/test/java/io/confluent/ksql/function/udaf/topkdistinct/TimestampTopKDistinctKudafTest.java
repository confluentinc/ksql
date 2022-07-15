/*
 * Copyright 2022 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class TimestampTopKDistinctKudafTest {
  private final List<Timestamp> valuesArray = ImmutableList.of(new Timestamp(10),
          new Timestamp(30), new Timestamp(45), new Timestamp(10), new Timestamp(50),
          new Timestamp(60), new Timestamp(20), new Timestamp(60), new Timestamp(80),
          new Timestamp(35), new Timestamp(25), new Timestamp(60), new Timestamp(80));
  private final TopkDistinctKudaf<Timestamp> timestampTopkDistinctKudaf
          = TopKDistinctTestUtils.getTopKDistinctKudaf(3, SqlTypes.TIMESTAMP);

  @Test
  public void shouldAggregateTopK() {
    List<Timestamp> currentVal = new ArrayList<>();
    for (final Timestamp d : valuesArray) {
      currentVal = timestampTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(
            new Timestamp(80), new Timestamp(60), new Timestamp(50))));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    List<Timestamp> currentVal = new ArrayList<>();
    currentVal = timestampTopkDistinctKudaf.aggregate(new Timestamp(80), currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(new Timestamp(80))));
  }

  @Test
  public void shouldMergeTopK() {
    final List<Timestamp> array1 = ImmutableList.of(new Timestamp(50), new Timestamp(45),
            new Timestamp(25));
    final List<Timestamp> array2 = ImmutableList.of(new Timestamp(60), new Timestamp(50),
            new Timestamp(48));

    assertThat("Invalid results.",
            timestampTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Timestamp(60), new Timestamp(50), new Timestamp(48))));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final List<Timestamp> array1 = ImmutableList.of(new Timestamp(50), new Timestamp(45));
    final List<Timestamp> array2 = ImmutableList.of(new Timestamp(60));

    assertThat("Invalid results.",
            timestampTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Timestamp(60), new Timestamp(50), new Timestamp(45))));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    final List<Timestamp> array1 = ImmutableList.of(new Timestamp(50), new Timestamp(45));
    final List<Timestamp> array2 = ImmutableList.of(new Timestamp(60), new Timestamp(50));

    assertThat("Invalid results.",
            timestampTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Timestamp(60), new Timestamp(50), new Timestamp(45))));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final List<Timestamp> array1 = ImmutableList.of(new Timestamp(60));
    final List<Timestamp> array2 = ImmutableList.of(new Timestamp(60));

    assertThat("Invalid results.",
            timestampTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Timestamp(60))));
  }
}
