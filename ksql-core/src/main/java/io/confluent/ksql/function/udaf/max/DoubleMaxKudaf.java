/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udaf.max;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

import java.util.Arrays;

import io.confluent.ksql.function.KsqlAggregateFunction;

public class DoubleMaxKudaf extends KsqlAggregateFunction<Double, Double> {

  public DoubleMaxKudaf(Integer argIndexInValue) {
    super(argIndexInValue, Double.MIN_VALUE, Schema.FLOAT64_SCHEMA,
          Arrays.asList(Schema.FLOAT64_SCHEMA),
          "MAX", DoubleMaxKudaf.class);
  }

  @Override
  public Double aggregate(Double currentVal, Double currentAggVal) {
    if (currentVal > currentAggVal) {
      return currentVal;
    }
    return currentAggVal;
  }

  @Override
  public Merger getMerger() {
    return new Merger<String, Double>() {
      @Override
      public Double apply(final String aggKey, final Double aggOne, final Double aggTwo) {
        if (aggOne > aggTwo) {
          return aggOne;
        }
        return aggTwo;
      }
    };
  }
}
