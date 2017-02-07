/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.function.udaf.sum;

import io.confluent.kql.physical.GenericRow;

import org.apache.kafka.streams.kstream.Aggregator;

import java.util.Map;


public class DoubleSum implements Aggregator<String, GenericRow, GenericRow> {

  Map<Integer, Integer> resultToSourceColumnMapAgg;
  Map<Integer, Integer> resultToSourceColumnMapNonAgg;

  public DoubleSum(final Map<Integer, Integer> resultToSourceColumnMapAgg,
                   final Map<Integer, Integer> resultToSourceColumnMapNonAgg) {
    this.resultToSourceColumnMapAgg = resultToSourceColumnMapAgg;
    this.resultToSourceColumnMapNonAgg = resultToSourceColumnMapNonAgg;
  }

  @Override
  public GenericRow apply(final String key, final GenericRow value, final GenericRow aggValue) {

    for (int resultAggColumnIndex : resultToSourceColumnMapAgg.keySet()) {
      Double currentResultValue = (Double) aggValue.getColumns().get(resultAggColumnIndex);
      Double
          currentValue =
          (Double) value.getColumns().get(resultToSourceColumnMapAgg.get(resultAggColumnIndex));
      aggValue.getColumns().set(resultAggColumnIndex, currentResultValue + currentValue);
    }

    for (int resultNonAggColumnIndex : resultToSourceColumnMapNonAgg.keySet()) {
      aggValue.getColumns().set(resultNonAggColumnIndex, value.getColumns()
          .get(resultToSourceColumnMapNonAgg.get(resultNonAggColumnIndex)));
    }
    System.out.println(key + " => " + aggValue);
    return aggValue;
  }
}