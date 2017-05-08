/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function;

import org.apache.kafka.connect.data.Schema;

import java.util.List;


public abstract class KSQLAggFunctionDeterminer {

  final String functionName;
  final List<KSQLAggregateFunction> aggregateFunctionList;

  public KSQLAggFunctionDeterminer(String functionName, List<KSQLAggregateFunction> aggregateFunctionList) {
    this.functionName = functionName;
    this.aggregateFunctionList = aggregateFunctionList;
  }

  public abstract KSQLAggregateFunction getProperAggregateFunction(List<Schema> argTypeList);

  public String getFunctionName() {
    return functionName;
  }

  public List<KSQLAggregateFunction> getAggregateFunctionList() {
    return aggregateFunctionList;
  }
}
