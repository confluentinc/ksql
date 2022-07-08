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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.sql.Date;
import java.util.Collections;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

public class DateMaxKudafTest {

  @Test
  public void shouldFindCorrectMax() {
    final MaxKudaf<Date> dateMaxKudaf = getMaxComparableKudaf();
    final Date[] values = new Date[]{new Date(3), new Date(5), new Date(8),
        new Date(2), new Date(3), new Date(4), new Date(5)};
    Date currentMax = null;
    for (final Date i: values) {
      currentMax = dateMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(new Date(8), equalTo(currentMax));
  }

  @Test
  public void shouldHandleNull() {
    final MaxKudaf<Date> dateMaxKudaf = getMaxComparableKudaf();
    final Date[] values = new Date[]{new Date(3), new Date(5), new Date(8), new Date(2),
        new Date(3), new Date(4), new Date(5)};
    Date currentMax = null;

    // null before any aggregation
    currentMax = dateMaxKudaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final Date i: values) {
      currentMax = dateMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(new Date(8), equalTo(currentMax));

    // null should not impact result
    currentMax = dateMaxKudaf.aggregate(null, currentMax);
    assertThat(new Date(8), equalTo(currentMax));
  }

  @Test
  public void shouldFindCorrectMaxForMerge() {
    final MaxKudaf dateMaxKudaf = getMaxComparableKudaf();
    final Merger<GenericKey, Date> merger = dateMaxKudaf.getMerger();
    final Date mergeResult1 = merger.apply(null, new Date(10), new Date(12));
    assertThat(mergeResult1, equalTo(new Date(12)));
    final Date mergeResult2 = merger.apply(null, new Date(10), new Date(-12));
    assertThat(mergeResult2, equalTo(new Date(10)));
    final Date mergeResult3 = merger.apply(null, new Date(-10), new Date(0));
    assertThat(mergeResult3, equalTo(new Date(0)));

  }

  private MaxKudaf getMaxComparableKudaf() {
    final KsqlAggregateFunction aggregateFunction = new MaxAggFunctionFactory()
        .createAggregateFunction(Collections.singletonList(SqlArgument.of(SqlTypes.DATE)),
            AggregateFunctionInitArguments.EMPTY_ARGS);
    assertThat(aggregateFunction, instanceOf(MaxKudaf.class));
    return  (MaxKudaf) aggregateFunction;
  }

}
