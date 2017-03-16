/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.function;

import org.apache.kafka.connect.data.Schema;
import java.util.List;


public abstract class KQLAggFunctionDeterminer {

  final String functionName;
  final List<KQLAggregateFunction> aggregateFunctionList;

  public KQLAggFunctionDeterminer(String functionName, List<KQLAggregateFunction> aggregateFunctionList) {
    this.functionName = functionName;
    this.aggregateFunctionList = aggregateFunctionList;
  }

  public abstract KQLAggregateFunction getProperAggregateFunction(List<Schema.Type> argTypeList);

  public String getFunctionName() {
    return functionName;
  }

  public List<KQLAggregateFunction> getAggregateFunctionList() {
    return aggregateFunctionList;
  }
}
