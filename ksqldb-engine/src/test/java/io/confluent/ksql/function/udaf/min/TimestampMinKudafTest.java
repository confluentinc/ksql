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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.sql.Timestamp;
import java.util.Collections;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

public class TimestampMinKudafTest {

  @Test
  public void shouldFindCorrectMin() {
    final MinKudaf<Timestamp> tsMinKudaf = getTimestampMinKudaf();
    final Timestamp[] values = new Timestamp[]{new Timestamp(3), new Timestamp(5), new Timestamp(8),
        new Timestamp(2), new Timestamp(3), new Timestamp(4), new Timestamp(5)};
    Timestamp currentMin = null;
    for (final Timestamp i: values) {
      currentMin = tsMinKudaf.aggregate(i, currentMin);
    }
    assertThat(new Timestamp(2), equalTo(currentMin));
  }

  @Test
  public void shouldHandleNull() {
    final MinKudaf<Timestamp> tsMinKudaf = getTimestampMinKudaf();
    final Timestamp[] values = new Timestamp[]{new Timestamp(3), new Timestamp(5), new Timestamp(8),
        new Timestamp(2), new Timestamp(3), new Timestamp(4), new Timestamp(5)};
    Timestamp currentMin = null;

    // aggregate null before any aggregation
    currentMin = tsMinKudaf.aggregate(null, currentMin);
    assertThat(null, equalTo(currentMin));

    // now send each value to aggregation and verify
    for (final Timestamp i: values) {
      currentMin = tsMinKudaf.aggregate(i, currentMin);
    }
    assertThat(new Timestamp(2), equalTo(currentMin));

    // null should not impact result
    currentMin = tsMinKudaf.aggregate(null, currentMin);
    assertThat(new Timestamp(2), equalTo(currentMin));
  }

  @Test
  public void shouldFindCorrectMinForMerge() {
    final MinKudaf tsMinKudaf = getTimestampMinKudaf();
    final Merger<GenericKey, Timestamp> merger = tsMinKudaf.getMerger();
    final Timestamp mergeResult1 = merger.apply(null, new Timestamp(10), new Timestamp(12));
    assertThat(mergeResult1, equalTo(new Timestamp(10L)));
    final Timestamp mergeResult2 = merger.apply(null, new Timestamp(10), new Timestamp(-12L));
    assertThat(mergeResult2, equalTo(new Timestamp(-12L)));
    final Timestamp mergeResult3 = merger.apply(null, new Timestamp(-10), new Timestamp(0));
    assertThat(mergeResult3, equalTo(new Timestamp(-10)));

  }


  private MinKudaf getTimestampMinKudaf() {
    final KsqlAggregateFunction aggregateFunction = new MinAggFunctionFactory()
        .createAggregateFunction(Collections.singletonList(SqlArgument.of(SqlTypes.TIMESTAMP)),
            AggregateFunctionInitArguments.EMPTY_ARGS);
    assertThat(aggregateFunction, instanceOf(MinKudaf.class));
    return  (MinKudaf) aggregateFunction;
  }
}
