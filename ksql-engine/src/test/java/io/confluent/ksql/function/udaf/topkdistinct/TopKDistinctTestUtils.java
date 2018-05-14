package io.confluent.ksql.function.udaf.topkdistinct;

import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.Collections;

public class TopKDistinctTestUtils {
  @SuppressWarnings("unchecked")
  public static <T extends Comparable<? super T>> TopkDistinctKudaf<T> getTopKDistinctKudaf(int topk, Schema schema) {
    Expression fooExpression = new QualifiedNameReference(QualifiedName.of("foo"));
    Expression topKExpression = new LongLiteral(Integer.toString(topk));
    return (TopkDistinctKudaf<T>) new TopkDistinctAggFunctionFactory()
        .getProperAggregateFunction(
            Collections.singletonList(schema))
        .getInstance(
            Collections.singletonMap("foo", 0),
            Arrays.asList(fooExpression, topKExpression));
  }
}
