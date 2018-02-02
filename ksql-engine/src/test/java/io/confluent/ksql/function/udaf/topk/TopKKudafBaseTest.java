package io.confluent.ksql.function.udaf.topk;

import io.confluent.ksql.function.KsqlAggregateFunction;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

public abstract class TopKKudafBaseTest {
    Object[] valueArray;
    TopkAggFunctionDeterminer topKFactory;
    List<Schema> argumentType;

    protected KsqlAggregateFunction<Object, Object[]> getTopKArgumentInstance() {
        return topKFactory.getProperAggregateFunction(argumentType);
    }
}
