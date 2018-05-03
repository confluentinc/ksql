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
import java.util.Map;

import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.KsqlException;

public class TopkKudaf<T extends Comparable<? super T>>
    extends BaseAggregateFunction<T, ArrayList<T>> {

  private final int topKSize;
  private final Class<T> clazz;
  private final Schema returnType;
  private final List<Schema> argumentTypes;

  @SuppressWarnings("unchecked")
  TopkKudaf(final int argIndexInValue,
            final int topKSize,
            final Schema returnType,
            final List<Schema> argumentTypes,
            final Class<T> clazz) {
    super(argIndexInValue, () -> new ArrayList<>(),
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
  public ArrayList<T> aggregate(final T currentVal, final ArrayList<T> currentAggValList) {
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
  public Merger<String, ArrayList<T>> getMerger() {
    return (aggKey, aggOneList, aggTwoList) -> {

      ArrayList<T> mergedList = new ArrayList<>(aggOneList);
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
  public KsqlAggregateFunction<T, ArrayList<T>> getInstance(final Map<String, Integer>
                                                                expressionNames,
                                                   final List<Expression> functionArguments) {
    if (functionArguments.size() != 2) {
      throw new KsqlException(String.format("Invalid parameter count. Need 2 args, got %d arg(s)",
                                            functionArguments.size()));
    }

    final int udafIndex = expressionNames.get(functionArguments.get(0).toString());
    final int topKSize = Integer.parseInt(functionArguments.get(1).toString());
    return new TopkKudaf<>(udafIndex, topKSize, returnType, argumentTypes, clazz);
  }
}