/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udaf.count;

import io.confluent.ksql.function.KsqlAggregateFunction;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

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

  @Override
  public Merger getMerger() {
    return new Merger<String, Long>() {
      @Override
      public Long apply(final String aggKey, final Long aggOne, final Long aggTwo) {
        return aggOne + aggTwo;
      }
    };
  }
}
