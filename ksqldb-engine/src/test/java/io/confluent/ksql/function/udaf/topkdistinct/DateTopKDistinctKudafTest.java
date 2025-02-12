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

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class DateTopKDistinctKudafTest {

  private final List<Date> valuesArray = ImmutableList.of(new Date(10), new Date(30), new Date(45),
          new Date(10), new Date(50), new Date(60), new Date(20), new Date(60), new Date(80),
          new Date(35), new Date(25), new Date(60), new Date(80));
  private final TopkDistinctKudaf<Date> dateTopkDistinctKudaf
          = TopKDistinctTestUtils.getTopKDistinctKudaf(3, SqlTypes.DATE);

  @Test
  public void shouldAggregateTopK() {
    List<Date> currentVal = new ArrayList<>();
    for (final Date d : valuesArray) {
      currentVal = dateTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal,
            equalTo(ImmutableList.of(new Date(80), new Date(60), new Date(50))));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    List<Date> currentVal = new ArrayList<>();
    currentVal = dateTopkDistinctKudaf.aggregate(new Date(80), currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(new Date(80))));
  }

  @Test
  public void shouldMergeTopK() {
    final List<Date> array1 = ImmutableList.of(new Date(50), new Date(45), new Date(25));
    final List<Date> array2 = ImmutableList.of(new Date(60), new Date(50), new Date(48));

    assertThat("Invalid results.", dateTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Date(60), new Date(50), new Date(48))));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final List<Date> array1 = ImmutableList.of(new Date(50), new Date(45));
    final List<Date> array2 = ImmutableList.of(new Date(60));

    assertThat("Invalid results.", dateTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Date(60), new Date(50), new Date(45))));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    final List<Date> array1 = ImmutableList.of(new Date(50), new Date(45));
    final List<Date> array2 = ImmutableList.of(new Date(60), new Date(50));

    assertThat("Invalid results.", dateTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Date(60), new Date(50), new Date(45))));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final List<Date> array1 = ImmutableList.of(new Date(60));
    final List<Date> array2 = ImmutableList.of(new Date(60));

    assertThat("Invalid results.", dateTopkDistinctKudaf.merge(array1, array2),
            equalTo(ImmutableList.of(new Date(60))));
  }
}
