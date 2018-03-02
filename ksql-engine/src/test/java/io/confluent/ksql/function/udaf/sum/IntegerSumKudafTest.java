package io.confluent.ksql.function.udaf.sum;


import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.Collections;

import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.min.IntegerMinKudaf;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class IntegerSumKudafTest {

  @Test
  public void shouldComputeCorrectSum() {
    KsqlAggregateFunction aggregateFunction = new SumAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(Schema.INT32_SCHEMA));
    assertThat(aggregateFunction, instanceOf(IntegerSumKudaf.class));
    IntegerSumKudaf integerSumKudaf = (IntegerSumKudaf) aggregateFunction;
    int[] values = new int[]{3, 5, 8, 2, 3, 4, 5};
    int currentMin = 0;
    for (int i: values) {
      currentMin = integerSumKudaf.aggregate(i, currentMin);
    }
    assertThat(30, equalTo(currentMin));
  }
}
