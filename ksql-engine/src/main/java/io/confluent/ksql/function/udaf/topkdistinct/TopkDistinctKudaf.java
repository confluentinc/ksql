/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.topkdistinct;

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;

public class TopkDistinctKudaf<T extends Comparable<? super T>>
    extends BaseAggregateFunction<T, List<T>> {

  private final int tkVal;
  private final Class<T> ttClass;
  private final Schema outputSchema;

  @SuppressWarnings("unchecked")
  TopkDistinctKudaf(
      final String functionName,
      final int argIndexInValue,
      final int tkVal,
      final Schema outputSchema,
      final Class<T> ttClass
  ) {
    super(
        functionName, argIndexInValue,
        ArrayList::new,
        SchemaBuilder.array(outputSchema).optional().build(),
        Collections.singletonList(outputSchema),
        "Calculates the Topk distinct values for a column, per key."
    );

    this.tkVal = tkVal;
    this.ttClass = ttClass;
    this.outputSchema = outputSchema;
  }

  @Override
  public List<T> aggregate(final T currentValue, final List<T> aggregateValue) {

    if (currentValue == null) {
      return aggregateValue;
    }

    final int currentSize = aggregateValue.size();
    if (currentSize == tkVal && currentValue.compareTo(aggregateValue.get(currentSize - 1)) <= 0) {
      return aggregateValue;
    }

    if (aggregateValue.contains(currentValue)) {
      return aggregateValue;
    }

    if (currentSize == tkVal) {
      aggregateValue.set(currentSize - 1, currentValue);
    } else {
      aggregateValue.add(currentValue);
    }

    aggregateValue.sort(Comparator.reverseOrder());
    return aggregateValue;
  }

  @Override
  public Merger<Struct, List<T>> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {
      final List<T> merged = new ArrayList<>(Math.min(tkVal, aggOne.size() + aggTwo.size()));

      int idx1 = 0;
      int idx2 = 0;
      for (int i = 0; i != tkVal; ++i) {
        final T v1 = getNextItem(aggOne, idx1);
        final T v2 = getNextItem(aggTwo, idx2);

        if (v1 == null && v2 == null) {
          break;
        }

        if (v1 != null && (v2 == null || v1.compareTo(v2) > 0)) {
          merged.add(v1);
          idx1++;
        } else if (v1 == null || v2.compareTo(v1) > 0) {
          merged.add(v2);
          idx2++;
        } else if (v1.compareTo(v2) == 0) {
          merged.add(v1);
          idx1++;
          idx2++;
        }
      }
      return merged;
    };
  }

  private static <T> T getNextItem(final List<T> aggList, final int idx) {
    return idx < aggList.size() ? aggList.get(idx) : null;
  }

  @Override
  public KsqlAggregateFunction<T, List<T>> getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    aggregateFunctionArguments.ensureArgCount(2, "TopkDistinct");
    final int udafIndex = aggregateFunctionArguments.udafIndex();
    final int tkValFromArg = Integer.parseInt(aggregateFunctionArguments.arg(1));
    return new TopkDistinctKudaf<>(functionName, udafIndex, tkValFromArg, outputSchema, ttClass);
  }

  Schema getOutputSchema() {
    return outputSchema;
  }
}