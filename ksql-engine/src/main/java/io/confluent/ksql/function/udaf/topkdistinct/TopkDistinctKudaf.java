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

package io.confluent.ksql.function.udaf.topkdistinct;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.kstream.Merger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.topk.TopkKudaf;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.ArrayUtil;
import io.confluent.ksql.util.KsqlException;

public class TopkDistinctKudaf<T> extends KsqlAggregateFunction<T, T[]> {
  private final int topKSize;
  private final Class<T> clazz;
  private T[] tempTopKArray;

  TopkDistinctKudaf(Integer argIndexInValue,
                    int topKSize,
                    Class<T> clazz) {
    super(argIndexInValue,
        () -> (T[]) new Object[topKSize],
          SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(),
          Arrays.asList(Schema.FLOAT64_SCHEMA)
    );
    this.topKSize = topKSize;
    this.tempTopKArray = (T[]) new Object[topKSize + 1];
    this.clazz = clazz;
  }

  @Override
  public T[] aggregate(T currentVal, T[] currentAggVal) {
    if (currentVal == null) {
      return currentAggVal;
    }
    if (ArrayUtil.containsValue(currentVal, currentAggVal)) {
      return currentAggVal;
    }
    int nullIndex = ArrayUtil.getNullIndex(currentAggVal);
    if (nullIndex != -1) {
      currentAggVal[nullIndex] = currentVal;
      Arrays.sort(currentAggVal, TopkKudaf.comparator());
      return currentAggVal;
    }

    System.arraycopy(currentAggVal, 0, tempTopKArray, 0, topKSize);
    tempTopKArray[topKSize] = currentVal;
    Arrays.sort(tempTopKArray, Collections.reverseOrder());
    return Arrays.copyOf(tempTopKArray, topKSize);
  }

  @Override
  public Merger<String, T[]> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {
      int nullId1 = ArrayUtil.getNullIndex(aggOne) == -1? topKSize : ArrayUtil.getNullIndex(aggOne);
      int nullId2 = ArrayUtil.getNullIndex(aggTwo) == -1? topKSize : ArrayUtil.getNullIndex(aggTwo);
      T[] tempMergeTopKArray = (T[]) new Object[nullId1 + nullId2];

      for (int i = 0; i < nullId1; i++) {
        tempMergeTopKArray[i] = aggOne[i];
      }
      int duplicateCount = 0;
      for (int i = nullId1; i < nullId1 + nullId2; i++) {
        if (ArrayUtil.containsValue(aggTwo[i - nullId1], aggOne)) {
          duplicateCount ++;
        } else {
          tempMergeTopKArray[i - duplicateCount] = aggTwo[i - nullId1];
        }
      }
      tempMergeTopKArray = ArrayUtil.getNoNullArray(clazz, tempMergeTopKArray);
      Arrays.sort(tempMergeTopKArray, Collections.reverseOrder());
      if (tempMergeTopKArray.length < topKSize) {
        tempMergeTopKArray = ArrayUtil.padWithNull(clazz, tempMergeTopKArray, topKSize);
        return tempMergeTopKArray;
      }
      return Arrays.copyOf(tempMergeTopKArray, topKSize);
    };
  }

  @Override
  public KsqlAggregateFunction<T, T[]> getInstance(Map<String, Integer> expressionNames,
                                                             List<Expression> functionArguments) {
    if (functionArguments.size() != 2) {
      throw new KsqlException(String.format("Invalid parameter count. Need 2 args, got %d arg(s).",
              functionArguments.size()));
    }
    int udafIndex = expressionNames.get(functionArguments.get(0).toString());
    int topKSize = Integer.parseInt(functionArguments.get(1).toString());
    return new TopkDistinctKudaf(udafIndex, topKSize, clazz);
  }

}