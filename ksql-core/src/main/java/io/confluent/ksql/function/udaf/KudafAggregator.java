/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.physical.GenericRow;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Merger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class KudafAggregator implements Aggregator<String, GenericRow, GenericRow> {

  Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap;
  Map<Integer, Integer> aggValToValColumnMap;

  public KudafAggregator(Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap, Map<Integer,
      Integer> aggValToValColumnMap) {
    this.aggValToAggFunctionMap = aggValToAggFunctionMap;
    this.aggValToValColumnMap = aggValToValColumnMap;
  }

  @Override
  public GenericRow apply(String s, GenericRow rowValue, GenericRow aggRowValue) {

    for (int aggValColIndex: aggValToValColumnMap.keySet()) {
      aggRowValue.getColumns().set(aggValColIndex, rowValue.getColumns()
          .get(aggValToValColumnMap.get(aggValColIndex)));
    }

    for (int aggFunctionIndex: aggValToAggFunctionMap.keySet()) {
      KsqlAggregateFunction ksqlAggregateFunction = aggValToAggFunctionMap.get(aggFunctionIndex);
      aggRowValue.getColumns().set(aggFunctionIndex, ksqlAggregateFunction.aggregate(
          rowValue.getColumns().get(ksqlAggregateFunction.getArgIndexInValue()),
          aggRowValue.getColumns().get(aggFunctionIndex))
      );
    }
    return aggRowValue;
  }

  public Merger getMerger() {
    return new Merger<String, GenericRow>() {
      @Override
      public GenericRow apply(String key, GenericRow aggRowOne, GenericRow aggRowTwo) {

        List columns = Stream.generate(String::new).limit(aggRowOne.getColumns().size()).collect
            (Collectors.toList());
        GenericRow mergedRow = new GenericRow(columns);

        for (int aggValColIndex: aggValToValColumnMap.keySet()) {
          if (aggRowOne.getColumns()
              .get(aggValToValColumnMap.get(aggValColIndex)).toString().length() > 0) {
            mergedRow.getColumns().set(aggValColIndex, aggRowOne.getColumns()
                .get(aggValToValColumnMap.get(aggValColIndex)));
          } else {
            mergedRow.getColumns().set(aggValColIndex, aggRowTwo.getColumns()
                .get(aggValToValColumnMap.get(aggValColIndex)));
          }

        }

        for (int aggFunctionIndex: aggValToAggFunctionMap.keySet()) {
          KsqlAggregateFunction ksqlAggregateFunction = aggValToAggFunctionMap.get(aggFunctionIndex);
          mergedRow.getColumns().set(aggFunctionIndex, ksqlAggregateFunction.getMerger()
              .apply( key,
                      aggRowOne.getColumns().get(aggFunctionIndex),
                      aggRowTwo.getColumns().get(aggFunctionIndex))
          );
        }
        return mergedRow;
      }
    };
  }

}
