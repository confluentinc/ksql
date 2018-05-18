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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.KsqlException;

public class TopkKudaf<T extends Comparable<? super T>>
    extends BaseAggregateFunction<T, List<T>> {

  private final int topKSize;
  private final Class<T> clazz;
  private final Schema returnType;
  private final List<Schema> argumentTypes;

  @SuppressWarnings("unchecked")
  TopkKudaf(final String functionName,
            final int argIndexInValue,
            final int topKSize,
            final Schema returnType,
            final List<Schema> argumentTypes,
            final Class<T> clazz) {
    super(functionName, argIndexInValue, () -> new ArrayList<>(),
          returnType,
          argumentTypes
    );
    this.topKSize = topKSize;
    this.returnType = returnType;
    this.argumentTypes = argumentTypes;
    this.clazz = clazz;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<T> aggregate(final T currentVal, final List<T> currentAggValList) {
    if (currentVal == null) {
      return currentAggValList;
    }

    currentAggValList.add(currentVal);
    Collections.sort(currentAggValList);
    Collections.reverse(currentAggValList);
    if (currentAggValList.size() > topKSize) {
      currentAggValList.remove(currentAggValList.size() - 1);
    }

    return currentAggValList;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Merger<String, List<T>> getMerger() {
    return (aggKey, aggOneList, aggTwoList) -> {

      List<T> mergedList = new ArrayList<>(aggOneList);
      mergedList.addAll(aggTwoList);
      Collections.sort(mergedList);
      Collections.reverse(mergedList);
      while (mergedList.size() > topKSize) {
        mergedList.remove(mergedList.size() - 1);
      }
      return mergedList;
    };
  }

  @Override
  public KsqlAggregateFunction<T, List<T>> getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    aggregateFunctionArguments.ensureArgCount(2, "TopK");
    final int udafIndex = aggregateFunctionArguments.udafIndex();
    final int topKSize = Integer.parseInt(aggregateFunctionArguments.arg(1));
    return new TopkKudaf<>(functionName, udafIndex, topKSize, returnType, argumentTypes, clazz);
  }
}