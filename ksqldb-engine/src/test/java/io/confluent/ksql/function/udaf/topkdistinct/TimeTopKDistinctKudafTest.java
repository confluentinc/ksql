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

import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class TimeTopKDistinctKudafTest {
  private final List<Time> valuesArray = ImmutableList.of(new Time(10), new Time(30), new Time(45),
          new Time(10), new Time(50), new Time(60), new Time(20), new Time(60), new Time(80),
          new Time(35), new Time(25), new Time(60), new Time(80));
  private final TopkDistinctKudaf<Time> timeTopkDistinctKudaf
          = TopKDistinctTestUtils.getTopKDistinctKudaf(3, SqlTypes.TIME);

  @Test
  public void shouldAggregateTopK() {
    List<Time> currentVal = new ArrayList<>();
    for (final Time d : valuesArray) {
      currentVal = timeTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(new Time(80), new Time(60),
            new Time(50))));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    List<Time> currentVal = new ArrayList<>();
    currentVal = timeTopkDistinctKudaf.aggregate(new Time(80), currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(new Time(80))));
  }

  @Test
  public void shouldMergeTopK() {
    final List<Time> array1 = ImmutableList.of(new Time(50), new Time(45), new Time(25));
    final List<Time> array2 = ImmutableList.of(new Time(60), new Time(50), new Time(48));

    assertThat("Invalid results.", timeTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Time(60), new Time(50), new Time(48))));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final List<Time> array1 = ImmutableList.of(new Time(50), new Time(45));
    final List<Time> array2 = ImmutableList.of(new Time(60));

    assertThat("Invalid results.", timeTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Time(60), new Time(50), new Time(45))));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    final List<Time> array1 = ImmutableList.of(new Time(50), new Time(45));
    final List<Time> array2 = ImmutableList.of(new Time(60), new Time(50));

    assertThat("Invalid results.", timeTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Time(60), new Time(50), new Time(45))));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final List<Time> array1 = ImmutableList.of(new Time(60));
    final List<Time> array2 = ImmutableList.of(new Time(60));

    assertThat("Invalid results.", timeTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Time(60))));
  }
}
