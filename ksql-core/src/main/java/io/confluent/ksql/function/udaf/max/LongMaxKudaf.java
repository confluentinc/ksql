/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udaf.max;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

import java.util.Arrays;

import io.confluent.ksql.function.KsqlAggregateFunction;

public class LongMaxKudaf extends KsqlAggregateFunction<Long, Long> {

  public LongMaxKudaf(Integer argIndexInValue) {
    super(argIndexInValue, 0L, Schema.INT64_SCHEMA,
          Arrays.asList(Schema.INT64_SCHEMA),
          "MAX", LongMaxKudaf.class);
  }

  @Override
  public Long aggregate(Long currentVal, Long currentAggVal) {
    if (currentVal > currentAggVal) {
      return currentVal;
    }
    return currentAggVal;
  }

  @Override
  public Merger getMerger() {
    return new Merger<String, Long>() {
      @Override
      public Long apply(final String aggKey, final Long aggOne, final Long aggTwo) {
        if (aggOne > aggTwo) {
          return aggOne;
        }
        return aggTwo;
      }
    };
  }
}
