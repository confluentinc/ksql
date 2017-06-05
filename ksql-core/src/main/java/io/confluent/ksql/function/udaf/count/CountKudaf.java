/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function.udaf.count;

import io.confluent.ksql.function.KsqlAggregateFunction;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;

public class CountKudaf extends KsqlAggregateFunction<Object, Long> {

  public CountKudaf(Integer argIndexInValue) {
    super(argIndexInValue, 0L, Schema.INT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
          "COUNT", CountKudaf.class);
  }

  @Override
  public Long aggregate(Object currentVal, Long currentAggVal) {
    return currentAggVal + 1;
  }
}
