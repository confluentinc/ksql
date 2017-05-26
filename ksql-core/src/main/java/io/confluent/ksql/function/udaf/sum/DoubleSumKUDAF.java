/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.KSQLAggregateFunction;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;

public class DoubleSumKUDAF extends KSQLAggregateFunction<Double, Double> {

  public DoubleSumKUDAF(Integer argIndexInValue) {
    super(argIndexInValue, 0.0, Schema.FLOAT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
          "SUM", DoubleSumKUDAF.class);
  }

  @Override
  public Double aggregate(Double currentVal, Double currentAggVal) {
    return currentVal + currentAggVal;
  }

}
