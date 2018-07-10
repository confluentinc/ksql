package io.confluent.ksql.function.udaf.sum;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.IntStream;

import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.min.IntegerMinKudaf;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class IntegerSumKudafTest extends BaseSumKudafTest<Integer, IntegerSumKudaf>{
  protected TGenerator<Integer> getTGenerator() {
    return Integer::valueOf;
  }

  protected IntegerSumKudaf getSumKudaf() {
    KsqlAggregateFunction aggregateFunction = new SumAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(Schema.OPTIONAL_INT32_SCHEMA));
    assertThat(aggregateFunction, instanceOf(IntegerSumKudaf.class));
    return  (IntegerSumKudaf) aggregateFunction;
  }
}
