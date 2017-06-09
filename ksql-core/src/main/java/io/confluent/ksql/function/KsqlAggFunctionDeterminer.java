/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function;

import org.apache.kafka.connect.data.Schema;

import java.util.List;


public abstract class KsqlAggFunctionDeterminer {

  final String functionName;
  final List<KsqlAggregateFunction> aggregateFunctionList;

  public KsqlAggFunctionDeterminer(String functionName,
                                   List<KsqlAggregateFunction> aggregateFunctionList) {
    this.functionName = functionName;
    this.aggregateFunctionList = aggregateFunctionList;
  }

  public abstract KsqlAggregateFunction getProperAggregateFunction(List<Schema> argTypeList);

  public String getFunctionName() {
    return functionName;
  }

  public List<KsqlAggregateFunction> getAggregateFunctionList() {
    return aggregateFunctionList;
  }
}
