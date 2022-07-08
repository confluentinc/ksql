/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collections;

import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

public class DecimalMaxKudafTest {

  @Test
  public void shouldFindCorrectMax() {
    final MaxKudaf<BigDecimal> decimalMaxKudaf = getMaxComparableKudaf(2);
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    BigDecimal currentMax = null;
    for (final double i: values) {
      currentMax = decimalMaxKudaf.aggregate(new BigDecimal(i, new MathContext(2)), currentMax);
    }
    assertThat(currentMax, is(new BigDecimal(8.0, new MathContext(2))));
  }

  @Test
  public void shouldHandleNull() {
    final MaxKudaf<BigDecimal> decimalMaxKudaf = getMaxComparableKudaf(2);
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    BigDecimal currentMax = null;

    // null before any aggregation
    currentMax = decimalMaxKudaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final double i: values) {
      currentMax = decimalMaxKudaf.aggregate(new BigDecimal(i, new MathContext(2)), currentMax);
    }
    assertThat(new BigDecimal(8.0, new MathContext(2)), equalTo(currentMax));

    // null should not impact result
    currentMax = decimalMaxKudaf.aggregate(null, currentMax);
    assertThat(new BigDecimal(8.0, new MathContext(2)), equalTo(currentMax));
  }

  @Test
  public void shouldFindCorrectMaxForMerge() {
    final MaxKudaf decimalMaxKudaf = getMaxComparableKudaf(3);
    final Merger<GenericKey, BigDecimal> merger = decimalMaxKudaf.getMerger();
    final BigDecimal mergeResult1 = merger.apply(null, new BigDecimal(10.0), new BigDecimal(12.0));
    assertThat(mergeResult1, equalTo(new BigDecimal(12.0, new MathContext(3))));
    final BigDecimal mergeResult2 = merger.apply(null, new BigDecimal(10.0), new BigDecimal(-12.0));
    assertThat(mergeResult2, equalTo(new BigDecimal(10.0, new MathContext(3))));
    final BigDecimal mergeResult3 = merger.apply(null, new BigDecimal(-10.0), new BigDecimal(0.0));
    assertThat(mergeResult3, equalTo(new BigDecimal(0.0, new MathContext(3))));
  }


  private MaxKudaf getMaxComparableKudaf(final int precision) {
    final KsqlAggregateFunction aggregateFunction = new MaxAggFunctionFactory()
        .createAggregateFunction(Collections.singletonList(SqlArgument.of(SqlDecimal.of(precision, 1)))
            , AggregateFunctionInitArguments.EMPTY_ARGS);
    assertThat(aggregateFunction, instanceOf(MaxKudaf.class));
    return  (MaxKudaf) aggregateFunction;
  }

}
