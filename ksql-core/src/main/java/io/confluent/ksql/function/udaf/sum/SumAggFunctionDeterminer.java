/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.KsqlAggFunctionDeterminer;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;

public class SumAggFunctionDeterminer extends KsqlAggFunctionDeterminer {

  public SumAggFunctionDeterminer() {
    super("SUM", Arrays.asList(new DoubleSumKUDAF(-1), new LongSumKUDAF(-1)));
  }

  @Override
  public KsqlAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    // For now we only support aggregate functions with one arg.
    for (KsqlAggregateFunction ksqlAggregateFunction : getAggregateFunctionList()) {
      if (ksqlAggregateFunction.getArguments().get(0) == argTypeList.get(0)) {
        return ksqlAggregateFunction;
      }
    }
    throw new KsqlException("No SUM aggregate function with " + argTypeList.get(0) + " "
                           + " argument type exists!");
  }
}
