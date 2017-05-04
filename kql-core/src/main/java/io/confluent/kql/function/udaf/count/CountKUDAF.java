/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.function.udaf.count;

import io.confluent.kql.function.KQLAggregateFunction;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;

public class CountKUDAF extends KQLAggregateFunction<Object, Integer> {

  public CountKUDAF(Integer argIndexInValue) {
    super(argIndexInValue, 0, Schema.INT32_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
          "COUNT", CountKUDAF.class);
  }

  @Override
  public Integer aggregate(Object currentVal, Integer currentAggVal) {
    return currentAggVal + 1;
  }
}
