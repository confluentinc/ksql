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

package io.confluent.ksql.function.udaf.max;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.sql.Time;
import java.util.Collections;
import org.junit.Test;

public class TimeMaxKudafTest {

  @Test
  public void shouldFindCorrectMax() {
    final MaxKudaf<Time> timeMaxKudaf = getMaxComparableKudaf();
    final Time[] values = new Time[]{new Time(3), new Time(5), new Time(8),
        new Time(2), new Time(3), new Time(4), new Time(5)};
    Time currentMax = null;
    for (final Time i: values) {
      currentMax = timeMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(new Time(8), equalTo(currentMax));
  }

  @Test
  public void shouldHandleNull() {
    final MaxKudaf<Time> timeMaxKudaf = getMaxComparableKudaf();
    final Time[] values = new Time[]{new Time(3), new Time(5), new Time(8), new Time(2),
        new Time(3), new Time(4), new Time(5)};
    Time currentMax = null;

    // null before any aggregation
    currentMax = timeMaxKudaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final Time i: values) {
      currentMax = timeMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(new Time(8), equalTo(currentMax));

    // null should not impact result
    currentMax = timeMaxKudaf.aggregate(null, currentMax);
    assertThat(new Time(8), equalTo(currentMax));
  }

  @Test
  public void shouldFindCorrectMaxForMerge() {
    final MaxKudaf<Time> timeMaxKudaf = getMaxComparableKudaf();
    final Time mergeResult1 = timeMaxKudaf.merge(new Time(10), new Time(12));
    assertThat(mergeResult1, equalTo(new Time(12)));
    final Time mergeResult2 = timeMaxKudaf.merge(new Time(10), new Time(-12));
    assertThat(mergeResult2, equalTo(new Time(10)));
    final Time mergeResult3 = timeMaxKudaf.merge(new Time(-10), new Time(0));
    assertThat(mergeResult3, equalTo(new Time(0)));
  }

  private MaxKudaf<Time> getMaxComparableKudaf() {
    final Udaf<Time, Time, Time> aggregateFunction = MaxKudaf.createMaxTime();
    aggregateFunction.initializeTypeArguments(
            Collections.singletonList(SqlArgument.of(SqlTypes.TIME))
    );
    assertThat(aggregateFunction, instanceOf(MaxKudaf.class));
    return  (MaxKudaf<Time>) aggregateFunction;
  }
}