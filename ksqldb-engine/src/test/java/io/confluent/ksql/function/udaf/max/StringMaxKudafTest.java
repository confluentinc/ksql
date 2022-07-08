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
import java.util.Collections;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

public class StringMaxKudafTest {

  @Test
  public void shouldFindCorrectMax() {
    final MaxKudaf<String> stringMaxKudaf = getMaxComparableKudaf();
    final String[] values = new String[]{"C", "F", "B", "E", "A", "D", "B"};
    String currentMax = "A";
    for (final String val : values) {
      currentMax = stringMaxKudaf.aggregate(val, currentMax);
    }
    assertThat("F", equalTo(currentMax));
  }

  @Test
  public void shouldHandleNull() {
    final MaxKudaf<String> stringMaxKudaf = getMaxComparableKudaf();
    final String[] values = new String[]{"C", "F", "B", "E", "A", "D", "B"};
    String currentMax = null;

    // null before any aggregation
    currentMax = stringMaxKudaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final String val : values) {
      currentMax = stringMaxKudaf.aggregate(val, currentMax);
    }
    assertThat("F", equalTo(currentMax));

    // null should not impact result
    currentMax = stringMaxKudaf.aggregate(null, currentMax);
    assertThat("F", equalTo(currentMax));
  }

  @Test
  public void shouldFindCorrectMaxForMerge() {
    final MaxKudaf<String> stringMaxKudaf = getMaxComparableKudaf();
    final Merger<GenericKey, String> merger = stringMaxKudaf.getMerger();
    final String mergeResult1 = merger.apply(null, "B", "D");
    assertThat(mergeResult1, equalTo("D"));
    final String mergeResult2 = merger.apply(null, "P", "F");
    assertThat(mergeResult2, equalTo("P"));
    final String mergeResult3 = merger.apply(null, "A", "K");
    assertThat(mergeResult3, equalTo("K"));
  }

  @SuppressWarnings("unchecked")
  private MaxKudaf<String> getMaxComparableKudaf() {
    final KsqlAggregateFunction<String, String , String> aggregateFunction =
        (KsqlAggregateFunction<String, String , String>) new MaxAggFunctionFactory()
        .createAggregateFunction(Collections.singletonList(SqlArgument.of(SqlTypes.STRING)),
            AggregateFunctionInitArguments.EMPTY_ARGS);
    assertThat(aggregateFunction, instanceOf(MaxKudaf.class));
    return  (MaxKudaf<String>) aggregateFunction;
  }
}