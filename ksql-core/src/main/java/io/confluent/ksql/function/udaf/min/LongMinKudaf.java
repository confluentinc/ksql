/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udaf.min;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

import java.util.Arrays;

import io.confluent.ksql.function.KsqlAggregateFunction;

public class LongMinKudaf extends KsqlAggregateFunction<Long, Long> {

  public LongMinKudaf(Integer argIndexInValue) {
    super(argIndexInValue, Long.MAX_VALUE, Schema.INT64_SCHEMA,
          Arrays.asList(Schema.INT64_SCHEMA),
          "MIN", LongMinKudaf.class);
  }

  @Override
  public Long aggregate(Long currentVal, Long currentAggVal) {
    if (currentVal < currentAggVal) {
      return currentVal;
    }
    return currentAggVal;
  }

  @Override
  public Merger getMerger() {
    return new Merger<String, Long>() {
      @Override
      public Long apply(final String aggKey, final Long aggOne, final Long aggTwo) {
        if (aggOne < aggTwo) {
          return aggOne;
        }
        return aggTwo;
      }
    };
  }
}
