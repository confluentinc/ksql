package io.confluent.ksql.function.udaf.topkdistinct;

import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.Collections;

import io.confluent.ksql.function.AggregateFunctionArguments;

public class TopKDistinctTestUtils {
  @SuppressWarnings("unchecked")
  public static <T extends Comparable<? super T>> TopkDistinctKudaf<T> getTopKDistinctKudaf(int topk, Schema schema) {
    return (TopkDistinctKudaf<T>) new TopkDistinctAggFunctionFactory()
        .getProperAggregateFunction(
            Collections.singletonList(schema))
        .getInstance(
            new AggregateFunctionArguments(Collections.singletonMap("foo", 0),
                Arrays.asList("foo", Integer.toString(topk))));
  }
}
