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
import java.sql.Date;
import java.util.Collections;
import org.junit.Test;

public class DateMinKudafTest {

  @Test
  public void shouldFindCorrectMin() {
    final MinKudaf<Date> dateMinKudaf = getDateMinKudaf();
    final Date[] values = new Date[]{new Date(3), new Date(5), new Date(8),
        new Date(2), new Date(3), new Date(4), new Date(5)};
    Date currentMin = null;
    for (final Date i: values) {
      currentMin = dateMinKudaf.aggregate(i, currentMin);
    }
    assertThat(new Date(2), equalTo(currentMin));
  }

  @Test
  public void shouldHandleNull() {
    final MinKudaf<Date> dateMinKudaf = getDateMinKudaf();
    final Date[] values = new Date[]{new Date(3), new Date(5), new Date(8),
        new Date(2), new Date(3), new Date(4), new Date(5)};
    Date currentMin = null;

    // aggregate null before any aggregation
    currentMin = dateMinKudaf.aggregate(null, currentMin);
    assertThat(null, equalTo(currentMin));

    // now send each value to aggregation and verify
    for (final Date i: values) {
      currentMin = dateMinKudaf.aggregate(i, currentMin);
    }
    assertThat(new Date(2), equalTo(currentMin));

    // null should not impact result
    currentMin = dateMinKudaf.aggregate(null, currentMin);
    assertThat(new Date(2), equalTo(currentMin));
  }

  @Test
  public void shouldFindCorrectMinForMerge() {
    final MinKudaf<Date> dateMinKudaf = getDateMinKudaf();
    final Date mergeResult1 = dateMinKudaf.merge(new Date(10), new Date(12));
    assertThat(mergeResult1, equalTo(new Date(10L)));
    final Date mergeResult2 = dateMinKudaf.merge(new Date(10), new Date(-12L));
    assertThat(mergeResult2, equalTo(new Date(-12L)));
    final Date mergeResult3 = dateMinKudaf.merge(new Date(-10), new Date(0));
    assertThat(mergeResult3, equalTo(new Date(-10)));
  }

  private MinKudaf<Date> getDateMinKudaf() {
    final Udaf<Date, Date, Date> aggregateFunction = MinKudaf.createMinDate();
    aggregateFunction.initializeTypeArguments(
            Collections.singletonList(SqlArgument.of(SqlTypes.DATE))
    );
    assertThat(aggregateFunction, instanceOf(MinKudaf.class));
    return  (MinKudaf<Date>) aggregateFunction;
  }
}