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

package io.confluent.ksql.function.udaf.topk;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.ParameterInfo;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.kstream.Merger;

public class TopkKudaf<T extends Comparable<? super T>>
    extends BaseAggregateFunction<T, List<T>, List<T>> {

  private final int topKSize;

  TopkKudaf(
      final String functionName,
      final int argIndexInValue,
      final int topKSize,
      final SqlType outputSchema,
      final List<ParamType> argumentTypes,
      final Class<T> clazz
  ) {
    super(
        functionName,
        argIndexInValue,
        ArrayList::new,
        outputSchema,
        outputSchema,
        argumentTypes
            .stream()
            .map(arg -> new ParameterInfo("val", arg, "", false))
            .collect(Collectors.toList()),
        "Calculates the TopK value for a column, per key."
    );
    this.topKSize = topKSize;
    Objects.requireNonNull(outputSchema);
  }

  @Override
  public List<T> aggregate(final T currentValue, final List<T> aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }

    final int currentSize = aggregateValue.size();
    if (!aggregateValue.isEmpty()) {
      final T last = aggregateValue.get(currentSize - 1);
      if (currentValue.compareTo(last) <= 0
          && currentSize == topKSize) {
        return aggregateValue;
      }
    }

    if (currentSize == topKSize) {
      aggregateValue.set(currentSize - 1, currentValue);
    } else {
      aggregateValue.add(currentValue);
    }

    aggregateValue.sort(Comparator.reverseOrder());
    return aggregateValue;
  }

  @Override
  public Merger<GenericKey, List<T>> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {
      final List<T> merged = new ArrayList<>(
          Math.min(topKSize, aggOne.size() + aggTwo.size()));

      int idx1 = 0;
      int idx2 = 0;
      for (int i = 0; i != topKSize; ++i) {
        final T v1 = idx1 < aggOne.size() ? aggOne.get(idx1) : null;
        final T v2 = idx2 < aggTwo.size() ? aggTwo.get(idx2) : null;

        if (v1 != null && (v2 == null || v1.compareTo(v2) >= 0)) {
          merged.add(v1);
          idx1++;
        } else if (v2 != null && (v1 == null || v1.compareTo(v2) < 0)) {
          merged.add(v2);
          idx2++;
        } else {
          break;
        }
      }

      return merged;
    };
  }

  @Override
  public Function<List<T>, List<T>> getResultMapper() {
    return Function.identity();
  }

}