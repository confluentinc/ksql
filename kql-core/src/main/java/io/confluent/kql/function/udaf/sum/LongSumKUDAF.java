/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.function.udaf.sum;

import org.apache.kafka.connect.data.Schema;
import java.util.Arrays;
import io.confluent.kql.function.KQLAggregateFunction;

public class LongSumKUDAF extends KQLAggregateFunction<Long, Long> {

  public LongSumKUDAF(Integer argIndexInValue) {
    super(argIndexInValue, 0L, Schema.INT64_SCHEMA, Arrays.asList(Schema.INT64_SCHEMA), "SUM",
          LongSumKUDAF.class);
  }
  @Override
  public Long aggregate(Long currentVal, Long currentAggVal) {
    return currentVal + currentAggVal;
  }
}
