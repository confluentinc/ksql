package io.confluent.ksql.function.udaf.sum;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;

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
