package io.confluent.ksql.function.udaf.sum;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;
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
    IntegerSumKudaf integerSumKudaf = getIntegerSumKudaf();
    int[] values = new int[]{3, 5, 8, 2, 3, 4, 5};
    int currentMin = 0;
    for (int i: values) {
      currentMin = integerSumKudaf.aggregate(i, currentMin);
    }
    assertThat(30, equalTo(currentMin));
  }

  @Test
  public void shouldComputeCorrectSumMerge() {
    IntegerSumKudaf integerSumKudaf = getIntegerSumKudaf();
    Merger<String, Integer> merger = integerSumKudaf.getMerger();
    Integer mergeResult1 = merger.apply("Key", 10, 12);
    assertThat(mergeResult1, equalTo(22));
    Integer mergeResult2 = merger.apply("Key", 10, -12);
    assertThat(mergeResult2, equalTo(-2));
    Integer mergeResult3 = merger.apply("Key", -10, 0);
    assertThat(mergeResult3, equalTo(-10));

  }

  private IntegerSumKudaf getIntegerSumKudaf() {
    KsqlAggregateFunction aggregateFunction = new SumAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(Schema.INT32_SCHEMA));
    assertThat(aggregateFunction, instanceOf(IntegerSumKudaf.class));
    return  (IntegerSumKudaf) aggregateFunction;
  }
}
