/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.function.udaf.count;

import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;

import io.confluent.kql.function.KQLAggregateFunction;

public class CountKUDAF extends KQLAggregateFunction<Object, Integer> {

  public CountKUDAF(Integer argIndexInValue) {
    super(argIndexInValue, 0, Schema.Type.FLOAT64, Arrays.asList(Schema.Type.FLOAT64),
          "COUNT", CountKUDAF.class);
  }

  @Override
  public Integer aggregate(Object currentVal, Integer currentAggVal) {
    return currentAggVal + 1;
  }
}
