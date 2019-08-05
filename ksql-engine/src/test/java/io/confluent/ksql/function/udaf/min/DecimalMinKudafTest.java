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

package io.confluent.ksql.function.udaf.min;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collections;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

public class DecimalMinKudafTest {

  @Test
  public void shouldFindCorrectMin() {
    final DecimalMinKudaf doubleMinKudaf = getDecimalMinKudaf(2);
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    BigDecimal currentMin = null;
    for (final double i: values) {
      currentMin = doubleMinKudaf.aggregate(new BigDecimal(i, new MathContext(2)), currentMin);
    }
    assertThat(currentMin, is(new BigDecimal(2.2, new MathContext(2))));
  }

  @Test
  public void shouldHandleNull() {
    final DecimalMinKudaf doubleMinKudaf = getDecimalMinKudaf(2);
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    BigDecimal currentMin = null;

    // null before any aggregation
    currentMin = doubleMinKudaf.aggregate(null, currentMin);
    assertThat(null, equalTo(currentMin));

    // now send each value to aggregation and verify
    for (final double i: values) {
      currentMin = doubleMinKudaf.aggregate(new BigDecimal(i, new MathContext(2)), currentMin);
    }
    assertThat(new BigDecimal(2.2, new MathContext(2)), equalTo(currentMin));

    // null should not impact result
    currentMin = doubleMinKudaf.aggregate(null, currentMin);
    assertThat(new BigDecimal(2.2, new MathContext(2)), equalTo(currentMin));
  }

  @Test
  public void shouldFindCorrectMinForMerge() {
    final DecimalMinKudaf doubleMinKudaf = getDecimalMinKudaf(3);
    final Merger<Struct, BigDecimal> merger = doubleMinKudaf.getMerger();
    final BigDecimal mergeResult1 = merger.apply(null, new BigDecimal(10.0), new BigDecimal(12.0));
    assertThat(mergeResult1, equalTo(new BigDecimal(10.0, new MathContext(3))));
    final BigDecimal mergeResult2 = merger.apply(null, new BigDecimal(10.0), new BigDecimal(-12.0));
    assertThat(mergeResult2, equalTo(new BigDecimal(-12.0, new MathContext(3))));
    final BigDecimal mergeResult3 = merger.apply(null, new BigDecimal(-10.0), new BigDecimal(0.0));
    assertThat(mergeResult3, equalTo(new BigDecimal(-10.0, new MathContext(3))));
  }


  private DecimalMinKudaf getDecimalMinKudaf(final int precision) {
    final KsqlAggregateFunction aggregateFunction = new MinAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(DecimalUtil.builder(precision, 1)));
    assertThat(aggregateFunction, instanceOf(DecimalMinKudaf.class));
    return  (DecimalMinKudaf) aggregateFunction;
  }

}
