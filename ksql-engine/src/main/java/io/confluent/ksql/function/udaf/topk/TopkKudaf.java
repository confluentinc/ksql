/**
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

import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.ArrayUtil;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TopkKudaf<T> extends KsqlAggregateFunction<T, T[]> {
  private final int topKSize;
  private final T[] tempTopKArray;
  private final Class<T> clazz;
  private final Schema initialValue;
  private final List<Schema> argumentTypes;

  TopkKudaf(int argIndexInValue,
            int topKSize,
            Schema initialValue,
            List<Schema> argumentTypes,
            Class<T> clazz) {
    super(argIndexInValue,
          (T[]) new Object[topKSize],
          initialValue,
          argumentTypes,
          "TOPK",
          TopkKudaf.class);
    this.topKSize = topKSize;
    this.tempTopKArray = (T[]) new Object[topKSize + 1];
    this.initialValue = initialValue;
    this.argumentTypes = argumentTypes;
    this.clazz = clazz;
  }

  @Override
  public T[] aggregate(final T currentVal, final T[] currentAggVal) {
    // TODO: For now we just use a simple algorithm. Maybe try finding a faster algorithm later
    if (currentVal == null) {
      return currentAggVal;
    }

    int nullIndex = ArrayUtil.getNullIndex(currentAggVal);
    if (nullIndex != -1) {
      currentAggVal[nullIndex] = currentVal;
      return currentAggVal;
    }
    System.arraycopy(currentAggVal, 0, tempTopKArray, 0, topKSize);
    tempTopKArray[topKSize] = currentVal;
    Arrays.sort(tempTopKArray, Collections.reverseOrder());
    return Arrays.copyOf(tempTopKArray, topKSize);
  }

  @Override
  public Merger<String, T[]> getMerger() {
    // TODO: For now we just use a simple algorithm. Maybe try finding a faster algorithm later
    return (aggKey, aggOne, aggTwo) -> {
      int nullId1 = ArrayUtil.getNullIndex(aggOne) == -1? topKSize : ArrayUtil.getNullIndex(aggOne);
      int nullId2 = ArrayUtil.getNullIndex(aggTwo) == -1? topKSize : ArrayUtil.getNullIndex(aggTwo);
      T[] tempMergeTopKArray = (T[]) new Object[nullId1 + nullId2];

      for (int i = 0; i < nullId1; i++) {
        tempMergeTopKArray[i] = aggOne[i];
      }
      for (int i = nullId1; i < nullId1 + nullId2; i++) {
        tempMergeTopKArray[i] = aggTwo[i - nullId1];
      }
      Arrays.sort(tempMergeTopKArray, Collections.reverseOrder());
      if (tempMergeTopKArray.length < topKSize) {
        tempMergeTopKArray = ArrayUtil.padWithNull(clazz, tempMergeTopKArray, topKSize);
        return tempMergeTopKArray;
      }
      return Arrays.copyOf(tempMergeTopKArray, topKSize);
    };
  }

  @Override
  public KsqlAggregateFunction<T, T[]> getInstance(final Map<String, Integer> expressionNames,
                                                   final List<Expression> functionArguments) {
    if (functionArguments.size() != 2) {
      throw new KsqlException(String.format("Invalid parameter count. Need 2 args, got %d arg(s)",
                              functionArguments.size()));
    }
    int udafIndex = expressionNames.get(functionArguments.get(0).toString());
    int topKSize = Integer.parseInt(functionArguments.get(1).toString());
    return new TopkKudaf(udafIndex, topKSize, initialValue, argumentTypes, clazz);
  }
}
