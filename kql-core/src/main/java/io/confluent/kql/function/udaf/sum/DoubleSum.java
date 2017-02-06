package io.confluent.kql.function.udaf.sum;

import io.confluent.kql.physical.GenericRow;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.Map;


public class DoubleSum implements Aggregator<String, GenericRow, GenericRow> {

    Map<Integer, Integer> resultToSourceColumnMap_agg;
    Map<Integer, Integer> resultToSourceColumnMap_nonAgg;

    public DoubleSum(Map<Integer, Integer> resultToSourceColumnMap_agg, Map<Integer, Integer> resultToSourceColumnMap_nonAgg) {
        this.resultToSourceColumnMap_agg = resultToSourceColumnMap_agg;
        this.resultToSourceColumnMap_nonAgg = resultToSourceColumnMap_nonAgg;
    }

    @Override
    public GenericRow apply(String key, GenericRow value, GenericRow aggValue) {

        for (int resultAggColumnIndex : resultToSourceColumnMap_agg.keySet()) {
            Double currentResultValue = (Double) aggValue.getColumns().get(resultAggColumnIndex);
            Double currentValue = (Double) value.getColumns().get(resultToSourceColumnMap_agg.get(resultAggColumnIndex));
            aggValue.getColumns().set(resultAggColumnIndex, currentResultValue + currentValue);
        }

        for (int resultNonAggColumnIndex: resultToSourceColumnMap_nonAgg.keySet()) {
            aggValue.getColumns().set(resultNonAggColumnIndex, value.getColumns().get(resultToSourceColumnMap_nonAgg.get(resultNonAggColumnIndex)));
        }

        System.out.println(key+" => "+aggValue);
        return aggValue;
    }
}