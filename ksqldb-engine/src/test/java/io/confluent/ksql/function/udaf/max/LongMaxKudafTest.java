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

package io.confluent.ksql.function.udaf.max;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collections;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

public class LongMaxKudafTest {

  @Test
  public void shouldFindCorrectMax() {
    final MaxKudaf<Long> longMaxKudaf = getMaxComparableKudaf();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    long currentMax = Long.MIN_VALUE;
    for (final long i: values) {
      currentMax = longMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(8L, equalTo(currentMax));
  }

  @Test
  public void shouldHandleNull() {
    final MaxKudaf<Long> longMaxKudaf = getMaxComparableKudaf();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    Long currentMax = null;

    // null before any aggregation
    currentMax = longMaxKudaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final long i: values) {
      currentMax = longMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(8L, equalTo(currentMax));

    // null should not impact result
    currentMax = longMaxKudaf.aggregate(null, currentMax);
    assertThat(8L, equalTo(currentMax));
  }

  @Test
  public void shouldFindCorrectMaxForMerge() {
    final MaxKudaf longMaxKudaf = getMaxComparableKudaf();
    final Merger<GenericKey, Long> merger = longMaxKudaf.getMerger();
    final Long mergeResult1 = merger.apply(null, 10L, 12L);
    assertThat(mergeResult1, equalTo(12L));
    final Long mergeResult2 = merger.apply(null, 10L, -12L);
    assertThat(mergeResult2, equalTo(10L));
    final Long mergeResult3 = merger.apply(null, -10L, 0L);
    assertThat(mergeResult3, equalTo(0L));

  }

  private MaxKudaf getMaxComparableKudaf() {
    final KsqlAggregateFunction aggregateFunction = new MaxAggFunctionFactory()
        .createAggregateFunction(Collections.singletonList(SqlArgument.of(SqlTypes.BIGINT)),
            AggregateFunctionInitArguments.EMPTY_ARGS);
    assertThat(aggregateFunction, instanceOf(MaxKudaf.class));
    return  (MaxKudaf) aggregateFunction;
  }

}
