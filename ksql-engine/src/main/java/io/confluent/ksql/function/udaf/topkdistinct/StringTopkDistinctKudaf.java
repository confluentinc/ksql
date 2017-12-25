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
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.ArrayUtil;
import io.confluent.ksql.util.KsqlException;

public class StringTopkDistinctKudaf extends KsqlAggregateFunction<String, String[]> {

  Integer tkVal;
  String[] topkArray;
  String[] tempTopkArray;

  public StringTopkDistinctKudaf(Integer argIndexInValue, Integer tkVal) {
    super(argIndexInValue, new String[tkVal], SchemaBuilder.array(Schema.STRING_SCHEMA).build(),
          Arrays.asList(Schema.STRING_SCHEMA),
          "TOPKDISTINCT", StringTopkDistinctKudaf.class);
    this.tkVal = tkVal;
    this.topkArray = new String[tkVal];
    this.tempTopkArray = new String[tkVal + 1];
  }

  @Override
  public String[] aggregate(String currentVal, String[] currentAggVal) {
    if (currentVal == null) {
      return currentAggVal;
    }
    if (ArrayUtil.containsValue(currentVal, currentAggVal)) {
      return currentAggVal;
    }
    int nullIndex = ArrayUtil.getNullIndex(currentAggVal);
    if (nullIndex != -1) {
      currentAggVal[nullIndex] = currentVal;
      return currentAggVal;
    }

    for (int i = 0; i < tkVal; i++) {
      tempTopkArray[i] = currentAggVal[i];
    }
    tempTopkArray[tkVal] = currentVal;
    Arrays.sort(tempTopkArray, Collections.reverseOrder());
    return Arrays.copyOf(tempTopkArray, tkVal);
  }

  @Override
  public Merger<String, String[]> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {

      int nullIndex1 = ArrayUtil.getNullIndex(aggOne) == -1? tkVal: ArrayUtil.getNullIndex(aggOne);
      int nullIndex2 = ArrayUtil.getNullIndex(aggTwo) == -1? tkVal: ArrayUtil.getNullIndex(aggTwo);
      String[] tempMergeTopkArray = new String[nullIndex1 + nullIndex2];

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
      tempMergeTopkArray = ArrayUtil.getNoNullArray(String.class, tempMergeTopkArray);
      Arrays.sort(tempMergeTopkArray, Collections.reverseOrder());
      if (tempMergeTopkArray.length < tkVal) {
        tempMergeTopkArray = ArrayUtil.padWithNull(String.class, tempMergeTopkArray, tkVal);
        return tempMergeTopkArray;
      }
      return Arrays.copyOf(tempMergeTopkArray, tkVal);
    };
  }

  @Override
  public KsqlAggregateFunction<String, String[]> getInstance(Map<String, Integer> expressionNames,
                                                         List<Expression> functionArguments) {
    if (functionArguments.size() != 2) {
      throw new KsqlException(String.format("Invalid parameter count. Need 2 args, got %d arg(s)"
                                            + ".", functionArguments.size()));
    }
    int udafIndex = expressionNames.get(functionArguments.get(0).toString());
    Integer tkValFromArg = Integer.parseInt(functionArguments.get(1).toString());
    StringTopkDistinctKudaf stringTopkDistinctKudaf = new StringTopkDistinctKudaf(udafIndex, tkValFromArg);
    return stringTopkDistinctKudaf;
  }

}