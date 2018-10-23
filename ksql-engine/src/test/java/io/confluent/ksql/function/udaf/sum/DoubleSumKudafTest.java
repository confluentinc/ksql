package io.confluent.ksql.function.udaf.sum;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;

public class DoubleSumKudafTest extends BaseSumKudafTest<Double, DoubleSumKudaf> {
  protected TGenerator<Double> getNumberGenerator() {
    return Double::valueOf;
  }

  protected DoubleSumKudaf getSumKudaf() {
    final KsqlAggregateFunction aggregateFunction = new SumAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA));
    assertThat(aggregateFunction, instanceOf(DoubleSumKudaf.class));
    return  (DoubleSumKudaf) aggregateFunction;
  }
}
