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

package io.confluent.ksql.function.udaf.min;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collections;
import org.junit.Test;

public class LongMinKudafTest {

  @Test
  public void shouldFindCorrectMin() {
    final MinKudaf<Long> longMinKudaf = getLongMinKudaf();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    long currentMin = Long.MAX_VALUE;
    for (final long i: values) {
      currentMin = longMinKudaf.aggregate(i, currentMin);
    }
    assertThat(2L, equalTo(currentMin));
  }

  @Test
  public void shouldHandleNull() {
    final MinKudaf<Long> longMinKudaf = getLongMinKudaf();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    Long currentMin = null;

    // aggregate null before any aggregation
    currentMin = longMinKudaf.aggregate(null, currentMin);
    assertThat(null, equalTo(currentMin));

    // now send each value to aggregation and verify
    for (final long i: values) {
      currentMin = longMinKudaf.aggregate(i, currentMin);
    }
    assertThat(2L, equalTo(currentMin));

    // null should not impact result
    currentMin = longMinKudaf.aggregate(null, currentMin);
    assertThat(2L, equalTo(currentMin));
  }

  @Test
  public void shouldFindCorrectMinForMerge() {
    final MinKudaf<Long> longMinKudaf = getLongMinKudaf();
    final Long mergeResult1 = longMinKudaf.merge(10L, 12L);
    assertThat(mergeResult1, equalTo(10L));
    final Long mergeResult2 = longMinKudaf.merge(10L, -12L);
    assertThat(mergeResult2, equalTo(-12L));
    final Long mergeResult3 = longMinKudaf.merge(-10L, 0L);
    assertThat(mergeResult3, equalTo(-10L));
  }

  private MinKudaf<Long> getLongMinKudaf() {
    final Udaf<Long, Long, Long> aggregateFunction = MinKudaf.createMinLong();
    aggregateFunction.initializeTypeArguments(
            Collections.singletonList(SqlArgument.of(SqlTypes.BIGINT))
    );
    assertThat(aggregateFunction, instanceOf(MinKudaf.class));
    return  (MinKudaf<Long>) aggregateFunction;
  }
}