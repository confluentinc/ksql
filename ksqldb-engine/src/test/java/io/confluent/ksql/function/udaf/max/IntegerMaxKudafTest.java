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

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collections;
import org.junit.Test;

public class IntegerMaxKudafTest {

  @Test
  public void shouldFindCorrectMax() {
    final MaxKudaf<Integer> integerMaxKudaf = getMaxComparableKudaf();
    final int[] values = new int[]{3, 5, 8, 2, 3, 4, 5};
    int currentMax = Integer.MIN_VALUE;
    for (final int i: values) {
      currentMax = integerMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(8, equalTo(currentMax));
  }

  @Test
  public void shouldHandleNull() {
    final MaxKudaf<Integer> integerMaxKudaf = getMaxComparableKudaf();
    final int[] values = new int[]{3, 5, 8, 2, 3, 4, 5};
    Integer currentMax = null;

    // null before any aggregation
    currentMax = integerMaxKudaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final int i: values) {
      currentMax = integerMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(8, equalTo(currentMax));

    // null should not impact result
    currentMax = integerMaxKudaf.aggregate(null, currentMax);
    assertThat(8, equalTo(currentMax));
  }

  @Test
  public void shouldFindCorrectMaxForMerge() {
    final MaxKudaf<Integer> integerMaxKudaf = getMaxComparableKudaf();
    final Integer mergeResult1 = integerMaxKudaf.merge(10, 12);
    assertThat(mergeResult1, equalTo(12));
    final Integer mergeResult2 = integerMaxKudaf.merge(10, -12);
    assertThat(mergeResult2, equalTo(10));
    final Integer mergeResult3 = integerMaxKudaf.merge(-10, 0);
    assertThat(mergeResult3, equalTo(0));
  }

  private MaxKudaf<Integer> getMaxComparableKudaf() {
    final Udaf<Integer, Integer, Integer> aggregateFunction = MaxKudaf.createMaxInt();
    aggregateFunction.initializeTypeArguments(
            Collections.singletonList(SqlArgument.of(SqlTypes.INTEGER))
    );
    assertThat(aggregateFunction, instanceOf(MaxKudaf.class));
    return  (MaxKudaf<Integer>) aggregateFunction;
  }
}