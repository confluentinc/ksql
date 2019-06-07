package io.confluent.ksql.function.udaf.topkdistinct;

import io.confluent.ksql.function.AggregateFunctionArguments;
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;

public class TopKDistinctTestUtils {
  @SuppressWarnings("unchecked")
  public static <T extends Comparable<? super T>> TopkDistinctKudaf<T> getTopKDistinctKudaf(
      final int topk, final Schema schema) {
    return (TopkDistinctKudaf<T>) new TopkDistinctAggFunctionFactory()
        .getProperAggregateFunction(
            Collections.singletonList(schema))
        .getInstance(
            new AggregateFunctionArguments(Collections.singletonMap("foo", 0),
                Arrays.asList("foo", Integer.toString(topk))));
  }
}
