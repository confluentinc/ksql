/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udaf.min;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.sql.Time;
import java.util.Collections;
import org.junit.Test;

public class TimeMinKudafTest {

  @Test
  public void shouldFindCorrectMin() {
    final MinKudaf<Time> timeMinKudaf = getTimeMinKudaf();
    final Time[] values = new Time[]{new Time(3), new Time(5), new Time(8),
        new Time(2), new Time(3), new Time(4), new Time(5)};
    Time currentMin = null;
    for (final Time i: values) {
      currentMin = timeMinKudaf.aggregate(i, currentMin);
    }
    assertThat(new Time(2), equalTo(currentMin));
  }

  @Test
  public void shouldHandleNull() {
    final MinKudaf<Time> timeMinKudaf = getTimeMinKudaf();
    final Time[] values = new Time[]{new Time(3), new Time(5), new Time(8),
        new Time(2), new Time(3), new Time(4), new Time(5)};
    Time currentMin = null;

    // aggregate null before any aggregation
    currentMin = timeMinKudaf.aggregate(null, currentMin);
    assertThat(null, equalTo(currentMin));

    // now send each value to aggregation and verify
    for (final Time i: values) {
      currentMin = timeMinKudaf.aggregate(i, currentMin);
    }
    assertThat(new Time(2), equalTo(currentMin));

    // null should not impact result
    currentMin = timeMinKudaf.aggregate(null, currentMin);
    assertThat(new Time(2), equalTo(currentMin));
  }

  @Test
  public void shouldFindCorrectMinForMerge() {
    final MinKudaf<Time> timeMinKudaf = getTimeMinKudaf();
    final Time mergeResult1 = timeMinKudaf.merge(new Time(10), new Time(12));
    assertThat(mergeResult1, equalTo(new Time(10L)));
    final Time mergeResult2 = timeMinKudaf.merge(new Time(10), new Time(-12L));
    assertThat(mergeResult2, equalTo(new Time(-12L)));
    final Time mergeResult3 = timeMinKudaf.merge(new Time(-10), new Time(0));
    assertThat(mergeResult3, equalTo(new Time(-10)));
  }

  private MinKudaf<Time> getTimeMinKudaf() {
    final Udaf<Time, Time, Time> aggregateFunction = MinKudaf.createMinTime();
    aggregateFunction.initializeTypeArguments(
            Collections.singletonList(SqlArgument.of(SqlTypes.TIME))
    );
    assertThat(aggregateFunction, instanceOf(MinKudaf.class));
    return (MinKudaf<Time>) aggregateFunction;
  }
}