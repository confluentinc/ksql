/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.KQLAggFunctionDeterminer;
import io.confluent.ksql.function.KQLAggregateFunction;
import io.confluent.ksql.util.KQLException;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;

public class SumAggFunctionDeterminer extends KQLAggFunctionDeterminer {

  public SumAggFunctionDeterminer() {
    super("SUM", Arrays.asList(new DoubleSumKUDAF(-1), new LongSumKUDAF(-1)));
  }

  @Override
  public KQLAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    // For now we only support aggregate functions with one arg.
    for (KQLAggregateFunction kqlAggregateFunction: getAggregateFunctionList()) {
      if (kqlAggregateFunction.getArguments().get(0) == argTypeList.get(0)) {
        return kqlAggregateFunction;
      }
    }
    throw new KQLException("No SUM aggregate function with " + argTypeList.get(0) + " "
                           + " argument type exists!");
  }
}
