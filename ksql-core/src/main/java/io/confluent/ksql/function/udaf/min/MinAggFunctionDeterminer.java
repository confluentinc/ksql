/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udaf.min;

import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;

import io.confluent.ksql.function.KsqlAggFunctionDeterminer;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.KsqlException;

public class MinAggFunctionDeterminer extends KsqlAggFunctionDeterminer {

  public MinAggFunctionDeterminer() {
    super("MIN", Arrays.asList(new DoubleMinKudaf(-1), new LongMinKudaf(-1)));
  }

  @Override
  public KsqlAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    // For now we only support aggregate functions with one arg.
    for (KsqlAggregateFunction ksqlAggregateFunction : getAggregateFunctionList()) {
      if (ksqlAggregateFunction.getArguments().get(0) == argTypeList.get(0)) {
        return ksqlAggregateFunction;
      }
    }
    throw new KsqlException("No Max aggregate function with " + argTypeList.get(0) + " "
                            + " argument type exists!");
  }
}
