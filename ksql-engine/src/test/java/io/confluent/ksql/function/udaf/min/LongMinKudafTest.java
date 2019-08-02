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
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

public class LongMinKudafTest {

  @Test
  public void shouldFindCorrectMin() {
    final LongMinKudaf longMinKudaf = getLongMinKudaf();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    long currentMin = Long.MAX_VALUE;
    for (final long i: values) {
      currentMin = longMinKudaf.aggregate(i, currentMin);
    }
    assertThat(2L, equalTo(currentMin));
  }

  @Test
  public void shouldHandleNull() {
    final LongMinKudaf longMinKudaf = getLongMinKudaf();
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
    final LongMinKudaf longMinKudaf = getLongMinKudaf();
    final Merger<Struct, Long> merger = longMinKudaf.getMerger();
    final Long mergeResult1 = merger.apply(null, 10L, 12L);
    assertThat(mergeResult1, equalTo(10L));
    final Long mergeResult2 = merger.apply(null, 10L, -12L);
    assertThat(mergeResult2, equalTo(-12L));
    final Long mergeResult3 = merger.apply(null, -10L, 0L);
    assertThat(mergeResult3, equalTo(-10L));

  }


  private LongMinKudaf getLongMinKudaf() {
    final KsqlAggregateFunction aggregateFunction = new MinAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA));
    assertThat(aggregateFunction, instanceOf(LongMinKudaf.class));
    return  (LongMinKudaf) aggregateFunction;
  }
}
