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
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

public class LongMaxKudafTest {

  @Test
  public void shouldFindCorrectMax() {
    final LongMaxKudaf longMaxKudaf = getLongMaxKudaf();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    long currentMax = Long.MIN_VALUE;
    for (final long i: values) {
      currentMax = longMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(8L, equalTo(currentMax));
  }

  @Test
  public void shouldHandleNull() {
    final LongMaxKudaf longMaxKudaf = getLongMaxKudaf();
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
    final LongMaxKudaf longMaxKudaf = getLongMaxKudaf();
    final Merger<Struct, Long> merger = longMaxKudaf.getMerger();
    final Long mergeResult1 = merger.apply(null, 10L, 12L);
    assertThat(mergeResult1, equalTo(12L));
    final Long mergeResult2 = merger.apply(null, 10L, -12L);
    assertThat(mergeResult2, equalTo(10L));
    final Long mergeResult3 = merger.apply(null, -10L, 0L);
    assertThat(mergeResult3, equalTo(0L));

  }

  private LongMaxKudaf getLongMaxKudaf() {
    final KsqlAggregateFunction aggregateFunction = new MaxAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA));
    assertThat(aggregateFunction, instanceOf(LongMaxKudaf.class));
    return  (LongMaxKudaf) aggregateFunction;
  }

}
