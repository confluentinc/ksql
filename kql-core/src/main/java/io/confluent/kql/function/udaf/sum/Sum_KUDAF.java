/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.kql.function.udaf.sum;

import io.confluent.kql.function.udaf.KUDAF;
import io.confluent.kql.physical.GenericRow;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;

import java.util.Map;

public class Sum_KUDAF extends KUDAF {
GenericRow resultGenericRow;
int aggColumnIndex;
Object aggColumnInitialValue;

Map<Integer, Integer> resultToSourceColumnMap_agg;
Map<Integer, Integer> resultToSourceColumnMap_nonAgg;

Initializer initializer;
Aggregator aggregator;

public Sum_KUDAF(final GenericRow resultGenericRow,
                 final int aggColumnIndex,
                 final Object aggColumnInitialValue,
                 final Map<Integer, Integer> resultToSourceColumnMap_agg,
                 final Map<Integer, Integer> resultToSourceColumnMap_nonAgg) {

    this.resultGenericRow = resultGenericRow;
    this.aggColumnIndex = aggColumnIndex;
    this.aggColumnInitialValue = aggColumnInitialValue;
    this.resultToSourceColumnMap_agg = resultToSourceColumnMap_agg;
    this.resultToSourceColumnMap_nonAgg = resultToSourceColumnMap_nonAgg;
    this.initializer = new DoubleSumInitializer(resultGenericRow, aggColumnIndex, aggColumnInitialValue);
    this.aggregator = new DoubleSum(resultToSourceColumnMap_agg, resultToSourceColumnMap_nonAgg);
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
