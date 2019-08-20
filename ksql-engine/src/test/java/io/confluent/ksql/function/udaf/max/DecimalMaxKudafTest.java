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

public class DecimalMaxKudafTest {

  @Test
  public void shouldFindCorrectMax() {
    final DecimalMaxKudaf doubleMaxKudaf = getDecimalMaxKudaf(2);
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    BigDecimal currentMax = null;
    for (final double i: values) {
      currentMax = doubleMaxKudaf.aggregate(new BigDecimal(i, new MathContext(2)), currentMax);
    }
    assertThat(currentMax, is(new BigDecimal(8.0, new MathContext(2))));
  }

  @Test
  public void shouldHandleNull() {
    final DecimalMaxKudaf doubleMaxKudaf = getDecimalMaxKudaf(2);
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    BigDecimal currentMax = null;

    // null before any aggregation
    currentMax = doubleMaxKudaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final double i: values) {
      currentMax = doubleMaxKudaf.aggregate(new BigDecimal(i, new MathContext(2)), currentMax);
    }
    assertThat(new BigDecimal(8.0, new MathContext(2)), equalTo(currentMax));

    // null should not impact result
    currentMax = doubleMaxKudaf.aggregate(null, currentMax);
    assertThat(new BigDecimal(8.0, new MathContext(2)), equalTo(currentMax));
  }

  @Test
  public void shouldFindCorrectMaxForMerge() {
    final DecimalMaxKudaf doubleMaxKudaf = getDecimalMaxKudaf(3);
    final Merger<Struct, BigDecimal> merger = doubleMaxKudaf.getMerger();
    final BigDecimal mergeResult1 = merger.apply(null, new BigDecimal(10.0), new BigDecimal(12.0));
    assertThat(mergeResult1, equalTo(new BigDecimal(12.0, new MathContext(3))));
    final BigDecimal mergeResult2 = merger.apply(null, new BigDecimal(10.0), new BigDecimal(-12.0));
    assertThat(mergeResult2, equalTo(new BigDecimal(10.0, new MathContext(3))));
    final BigDecimal mergeResult3 = merger.apply(null, new BigDecimal(-10.0), new BigDecimal(0.0));
    assertThat(mergeResult3, equalTo(new BigDecimal(0.0, new MathContext(3))));
  }


  private DecimalMaxKudaf getDecimalMaxKudaf(final int precision) {
    final KsqlAggregateFunction aggregateFunction = new MaxAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(DecimalUtil.builder(precision, 1)));
    assertThat(aggregateFunction, instanceOf(DecimalMaxKudaf.class));
    return  (DecimalMaxKudaf) aggregateFunction;
  }

}
