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

import java.lang.reflect.Array;
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

  private Integer tkVal;
  private T[] tempTopkArray;
  private Class<T> ttClass;

  TopkDistinctKudaf(Integer argIndexInValue,
                    Integer tkVal,
                    Class<T> ttClass) {
    super(argIndexInValue,
        () -> (T[]) Array.newInstance(ttClass, tkVal),
          SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(),
          Arrays.asList(Schema.FLOAT64_SCHEMA)
    );

    this.tkVal = tkVal;
    this.tempTopkArray = (T[]) Array.newInstance(ttClass,tkVal + 1);
    this.ttClass = ttClass;
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

    System.arraycopy(currentAggVal, 0, tempTopkArray, 0, tkVal);
    tempTopkArray[tkVal] = currentVal;
    Arrays.sort(tempTopkArray, Collections.reverseOrder());
    return Arrays.copyOf(tempTopkArray, tkVal);
  }

  @Override
  public Merger<String, T[]> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {

      int nullIndex1 = ArrayUtil.getNullIndex(aggOne) == -1? tkVal: ArrayUtil.getNullIndex(aggOne);
      int nullIndex2 = ArrayUtil.getNullIndex(aggTwo) == -1? tkVal: ArrayUtil.getNullIndex(aggTwo);
      T[] tempMergeTopkArray = (T[]) Array.newInstance(ttClass, nullIndex1 + nullIndex2);

      for (int i = 0; i < nullIndex1; i++) {
        tempMergeTopkArray[i] = aggOne[i];
      }
      int duplicateCount = 0;
      for (int i = nullIndex1; i < nullIndex1 + nullIndex2; i++) {
        if (ArrayUtil.containsValue(aggTwo[i - nullIndex1], aggOne)) {
          duplicateCount ++;
        } else {
          tempMergeTopkArray[i - duplicateCount] = aggTwo[i - nullIndex1];
        }
      }
      tempMergeTopkArray = ArrayUtil.getNoNullArray(ttClass, tempMergeTopkArray);
      Arrays.sort(tempMergeTopkArray, Collections.reverseOrder());
      if (tempMergeTopkArray.length < tkVal) {
        tempMergeTopkArray = ArrayUtil.padWithNull(ttClass, tempMergeTopkArray, tkVal);
        return tempMergeTopkArray;
      }
      return Arrays.copyOf(tempMergeTopkArray, tkVal);
    };
  }

  @Override
  public KsqlAggregateFunction<T, T[]> getInstance(Map<String, Integer> expressionNames,
                                                             List<Expression> functionArguments) {
    if (functionArguments.size() != 2) {
      throw new KsqlException(String.format("Invalid parameter count. Need 2 args, got %d arg(s)"
                                            + ".", functionArguments.size()));
    }
    int udafIndex = expressionNames.get(functionArguments.get(0).toString());
    Integer tkValFromArg = Integer.parseInt(functionArguments.get(1).toString());
    return new TopkDistinctKudaf(udafIndex, tkValFromArg, ttClass);
  }

}