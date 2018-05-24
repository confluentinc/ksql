/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udaf.topk;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.ArrayUtil;

public class TopkKudaf<T extends Comparable<? super T>> extends BaseAggregateFunction<T, T[]> {

  private final int topKSize;
  private final Class<T> clazz;
  private final Schema returnType;
  private final List<Schema> argumentTypes;
  private final Comparator<T> comparator;

  @SuppressWarnings("unchecked")
  TopkKudaf(final String functionName,
            final int argIndexInValue,
            final int topKSize,
            final Schema returnType,
            final List<Schema> argumentTypes,
            final Class<T> clazz) {
    super(functionName,
        argIndexInValue,
        () -> (T[]) Array.newInstance(clazz, topKSize),
        returnType,
        argumentTypes
    );
    this.topKSize = topKSize;
    this.returnType = returnType;
    this.argumentTypes = argumentTypes;
    this.clazz = clazz;
    this.comparator = (v1, v2) -> {
      if (v1 == null && v2 == null) {
        return 0;
      }
      if (v1 == null) {
        return 1;
      }
      if (v2 == null) {
        return -1;
      }

      return Comparator.<T>reverseOrder().compare(v1, v2);
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public T[] aggregate(final T currentValue, final T[] aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }

    final T last = aggregateValue[aggregateValue.length - 1];
    if (last != null && currentValue.compareTo(last) <= 0) {
      return aggregateValue;
    }

    final int nullIndex = ArrayUtil.getNullIndex(aggregateValue);
    if (nullIndex != -1) {
      aggregateValue[nullIndex] = currentValue;
      Arrays.sort(aggregateValue, comparator);
      return aggregateValue;
    }

    aggregateValue[aggregateValue.length - 1] = currentValue;
    Arrays.sort(aggregateValue, comparator);
    return aggregateValue;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Merger<String, T[]> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {
      final T[] merged = (T[]) Array.newInstance(clazz, topKSize);

      int idx1 = 0;
      int idx2 = 0;
      for (int i = 0; i != topKSize; ++i) {
        final T v1 = idx1 < aggOne.length ? aggOne[idx1] : null;
        final T v2 = idx2 < aggTwo.length ? aggTwo[idx2] : null;

        if (comparator.compare(v1, v2) < 0) {
          merged[i] = v1;
          idx1++;
        } else {
          merged[i] = v2;
          idx2++;
        }
      }

      return merged;
    };
  }

  @Override
  public KsqlAggregateFunction<T, T[]> getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    aggregateFunctionArguments.ensureArgCount(2, "TopK");
    final int udafIndex = aggregateFunctionArguments.udafIndex();
    final int topKSize = Integer.parseInt(aggregateFunctionArguments.arg(1));
    return new TopkKudaf<>(functionName, udafIndex, topKSize, returnType, argumentTypes, clazz);
  }
}
