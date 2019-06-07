package io.confluent.ksql.function.udaf.sum;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;

public class LongSumKudafTest extends BaseSumKudafTest<Long, LongSumKudaf> {
  protected TGenerator<Long> getNumberGenerator() {
    return Long::valueOf;
  }

  protected LongSumKudaf getSumKudaf() {
    final KsqlAggregateFunction aggregateFunction = new SumAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA));
    assertThat(aggregateFunction, instanceOf(LongSumKudaf.class));
    return  (LongSumKudaf) aggregateFunction;
  }
}
