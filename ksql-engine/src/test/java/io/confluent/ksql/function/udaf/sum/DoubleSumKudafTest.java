package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.KsqlAggregateFunction;
import org.apache.kafka.connect.data.Schema;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class DoubleSumKudafTest extends BaseSumKudafTest<Double, DoubleSumKudaf> {
  protected TGenerator<Double> getTGenerator() {
    return Double::valueOf;
  }

  protected DoubleSumKudaf getSumKudaf() {
    KsqlAggregateFunction aggregateFunction = new SumAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA));
    assertThat(aggregateFunction, instanceOf(DoubleSumKudaf.class));
    return  (DoubleSumKudaf) aggregateFunction;
  }
}
