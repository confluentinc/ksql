/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function.udaf.count;

import io.confluent.ksql.function.KQLAggFunctionDeterminer;
import io.confluent.ksql.function.KQLAggregateFunction;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;

public class CountAggFunctionDeterminer extends KQLAggFunctionDeterminer {

  public CountAggFunctionDeterminer() {
    super("COUNT", Arrays.asList(new CountKUDAF(-1)));
  }

  @Override
  public KQLAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    return getAggregateFunctionList().get(0);
  }
}
