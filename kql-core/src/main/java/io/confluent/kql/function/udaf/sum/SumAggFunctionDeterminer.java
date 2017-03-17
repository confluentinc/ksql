/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.function.udaf.sum;

import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;

import io.confluent.kql.function.KQLAggFunctionDeterminer;
import io.confluent.kql.function.KQLAggregateFunction;
import io.confluent.kql.util.KQLException;

/**
 * Created by hojjat on 3/16/17.
 */
public class SumAggFunctionDeterminer extends KQLAggFunctionDeterminer {

  public SumAggFunctionDeterminer() {
    super("SUM", Arrays.asList(new DoubleSumKUDAF(-1), new LongSumKUDAF(-1)));
  }

  @Override
  public KQLAggregateFunction getProperAggregateFunction(List<Schema.Type> argTypeList) {
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
