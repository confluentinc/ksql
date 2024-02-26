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

public class DoubleMinKudafTest {

  @Test
  public void shouldFindCorrectMin() {
    final MinKudaf<Double> doubleMinKudaf = getDoubleMinKudaf();
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    double currentMin = Double.MAX_VALUE;
    for (final double i: values) {
      currentMin = doubleMinKudaf.aggregate(i, currentMin);
    }
    assertThat(2.2, equalTo(currentMin));
  }

  @Test
  public void shouldHandleNull() {
    final MinKudaf<Double> doubleMinKudaf = getDoubleMinKudaf();
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    Double currentMin = null;

    // null before any aggregation
    currentMin = doubleMinKudaf.aggregate(null, currentMin);
    assertThat(null, equalTo(currentMin));

    // now send each value to aggregation and verify
    for (final double i: values) {
      currentMin = doubleMinKudaf.aggregate(i, currentMin);
    }
    assertThat(2.2, equalTo(currentMin));

    // null should not impact result
    currentMin = doubleMinKudaf.aggregate(null, currentMin);
    assertThat(2.2, equalTo(currentMin));
  }

  @Test
  public void shouldFindCorrectMinForMerge() {
    final MinKudaf<Double> doubleMinKudaf = getDoubleMinKudaf();
    final Double mergeResult1 = doubleMinKudaf.merge(10.0, 12.0);
    assertThat(mergeResult1, equalTo(10.0));
    final Double mergeResult2 = doubleMinKudaf.merge(10.0, -12.0);
    assertThat(mergeResult2, equalTo(-12.0));
    final Double mergeResult3 = doubleMinKudaf.merge(-10.0, 0.0);
    assertThat(mergeResult3, equalTo(-10.0));
  }

  private MinKudaf<Double> getDoubleMinKudaf() {
    final Udaf<Double, Double, Double> aggregateFunction = MinKudaf.createMinDouble();
    aggregateFunction.initializeTypeArguments(
            Collections.singletonList(SqlArgument.of(SqlTypes.DOUBLE))
    );
    assertThat(aggregateFunction, instanceOf(MinKudaf.class));
    return (MinKudaf<Double>) aggregateFunction;
  }
}