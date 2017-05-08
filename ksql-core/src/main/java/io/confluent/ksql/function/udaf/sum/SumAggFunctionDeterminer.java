/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.KSQLAggFunctionDeterminer;
import io.confluent.ksql.function.KSQLAggregateFunction;
import io.confluent.ksql.util.KSQLException;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;

public class SumAggFunctionDeterminer extends KSQLAggFunctionDeterminer {

  public SumAggFunctionDeterminer() {
    super("SUM", Arrays.asList(new DoubleSumKUDAF(-1), new LongSumKUDAF(-1)));
  }

  @Override
  public KSQLAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    // For now we only support aggregate functions with one arg.
    for (KSQLAggregateFunction ksqlAggregateFunction : getAggregateFunctionList()) {
      if (ksqlAggregateFunction.getArguments().get(0) == argTypeList.get(0)) {
        return ksqlAggregateFunction;
      }
    }
    throw new KSQLException("No SUM aggregate function with " + argTypeList.get(0) + " "
                           + " argument type exists!");
  }
}
