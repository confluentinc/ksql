/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function.udaf;

import io.confluent.ksql.function.KSQLAggregateFunction;
import io.confluent.ksql.physical.GenericRow;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.Map;

public class KUDAFAggregator implements Aggregator<String, GenericRow, GenericRow> {

  Map<Integer, KSQLAggregateFunction> aggValToAggFunctionMap;
  Map<Integer, Integer> aggValToValColumnMap;

  public KUDAFAggregator(Map<Integer, KSQLAggregateFunction> aggValToAggFunctionMap, Map<Integer,
      Integer> aggValToValColumnMap) {
    this.aggValToAggFunctionMap = aggValToAggFunctionMap;
    this.aggValToValColumnMap = aggValToValColumnMap;
  }

  @Override
  public GenericRow apply(String s, GenericRow rowValue, GenericRow aggRowValue) {

    for (int aggValColIndex: aggValToValColumnMap.keySet()) {
      aggRowValue.getColumns().set(aggValColIndex, rowValue.getColumns().get(aggValToValColumnMap.get(aggValColIndex)));
    }

    for (int aggFunctionIndex: aggValToAggFunctionMap.keySet()) {
      KSQLAggregateFunction ksqlAggregateFunction = aggValToAggFunctionMap.get(aggFunctionIndex);
      aggRowValue.getColumns().set(aggFunctionIndex, ksqlAggregateFunction.aggregate(
          rowValue.getColumns().get(ksqlAggregateFunction.getArgIndexInValue()), aggRowValue.getColumns().get(aggFunctionIndex))
      );
    }
    return aggRowValue;
  }
}
