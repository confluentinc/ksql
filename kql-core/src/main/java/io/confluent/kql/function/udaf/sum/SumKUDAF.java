/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.function.udaf.sum;

import io.confluent.kql.function.udaf.KUDAF;
import io.confluent.kql.physical.GenericRow;

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;

import java.util.Map;

public class SumKUDAF extends KUDAF {

  GenericRow resultGenericRow;
  int aggColumnIndex;
  Object aggColumnInitialValue;

  Map<Integer, Integer> resultToSourceColumnMapAgg;
  Map<Integer, Integer> resultToSourceColumnMapNonAgg;

  Initializer initializer;
  Aggregator aggregator;

  public SumKUDAF(final GenericRow resultGenericRow,
                   final int aggColumnIndex,
                   final Object aggColumnInitialValue,
                   final Map<Integer, Integer> resultToSourceColumnMapAgg,
                   final Map<Integer, Integer> resultToSourceColumnMapNonAgg) {

    this.resultGenericRow = resultGenericRow;
    this.aggColumnIndex = aggColumnIndex;
    this.aggColumnInitialValue = aggColumnInitialValue;
    this.resultToSourceColumnMapAgg = resultToSourceColumnMapAgg;
    this.resultToSourceColumnMapNonAgg = resultToSourceColumnMapNonAgg;
    this.initializer =
        new DoubleSumInitializer(resultGenericRow, aggColumnIndex, aggColumnInitialValue);
    this.aggregator = new DoubleSum(resultToSourceColumnMapAgg, resultToSourceColumnMapNonAgg);
  }

  @Override
  public Initializer getInitializer() {
    return initializer;
  }

  @Override
  public Aggregator getAggregator() {
    return aggregator;
  }
}
