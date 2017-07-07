/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.KsqlAggregateFunction;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

import java.util.Arrays;

public class LongSumKudaf extends KsqlAggregateFunction<Long, Long> {

  public LongSumKudaf(Integer argIndexInValue) {
    super(argIndexInValue, 0L, Schema.INT64_SCHEMA,
          Arrays.asList(Schema.INT64_SCHEMA), "SUM", LongSumKudaf.class);
  }

  @Override
  public Long aggregate(Long currentVal, Long currentAggVal) {
    return currentVal + currentAggVal;
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
