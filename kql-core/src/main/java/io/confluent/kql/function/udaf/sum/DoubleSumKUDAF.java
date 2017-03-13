package io.confluent.kql.function.udaf.sum;

import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;

import io.confluent.kql.function.KQLAggregateFunction;

public class DoubleSumKUDAF extends KQLAggregateFunction<Double, Double> {

  public DoubleSumKUDAF(Integer argIndexInValue) {
    super(argIndexInValue, 0.0, Schema.Type.FLOAT64, Arrays.asList(Schema.Type.FLOAT64),
          "SUM", DoubleSumKUDAF.class);
  }

  @Override
  public Double aggregate(Double currentVal, Double currentAggVal) {
    return currentVal + currentAggVal;
  }

}
