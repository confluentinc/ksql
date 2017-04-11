/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.function.udaf.count;

import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;

import io.confluent.kql.function.KQLAggFunctionDeterminer;
import io.confluent.kql.function.KQLAggregateFunction;

public class CountAggFunctionDeterminer extends KQLAggFunctionDeterminer {

  public CountAggFunctionDeterminer() {
    super("COUNT", Arrays.asList(new CountKUDAF(-1)));
  }

  @Override
  public KQLAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    return getAggregateFunctionList().get(0);
  }
}
