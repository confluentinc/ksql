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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.ParameterInfo;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.streams.kstream.Merger;

public class TopkDistinctKudaf<T extends Comparable<? super T>>
    extends BaseAggregateFunction<T, List<T>, List<T>> {

  private final int tkVal;

  TopkDistinctKudaf(
      final String functionName,
      final int argIndexInValue,
      final int tkVal,
      final SqlType outputSchema,
      final ParamType paramType,
      final Class<T> ttClass
  ) {
    super(
        functionName,
        argIndexInValue,
        ArrayList::new,
        SqlTypes.array(outputSchema),
        SqlTypes.array(outputSchema),
        Collections.singletonList(new ParameterInfo("val", paramType, "", false)),
        "Calculates the Topk distinct values for a column, per key."
    );

    this.tkVal = tkVal;
    Objects.requireNonNull(outputSchema);
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
  public Merger<GenericKey, List<T>> getMerger() {
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

  @Override
  public Function<List<T>, List<T>> getResultMapper() {
    return Function.identity();
  }

  private static <T> T getNextItem(final List<T> aggList, final int idx) {
    return idx < aggList.size() ? aggList.get(idx) : null;
  }

}