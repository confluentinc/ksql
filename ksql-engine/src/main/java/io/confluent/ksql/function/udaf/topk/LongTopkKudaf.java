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


public class LongTopkKudaf extends KsqlAggregateFunction<Long, Long[]> {

  Integer tkVal;
  Long[] topkArray;
  Long[] tempTopkArray;

  public LongTopkKudaf(Integer argIndexInValue, Integer tkVal) {
    super(argIndexInValue, new Long[tkVal], SchemaBuilder.array(Schema.INT64_SCHEMA).build(),
          Arrays.asList(Schema.INT64_SCHEMA),
          "TOPK", LongTopkKudaf.class);
    this.tkVal = tkVal;
    this.topkArray = new Long[tkVal];
    this.tempTopkArray = new Long[tkVal + 1];
  }

  @Override
  public Long[] aggregate(Long currentVal, Long[] currentAggVal) {
    if (currentVal == null) {
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
  public Merger<String, Long[]> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {
      int nullIndex1 = ArrayUtil.getNullIndex(aggOne) == -1? tkVal: ArrayUtil.getNullIndex(aggOne);
      int nullIndex2 = ArrayUtil.getNullIndex(aggTwo) == -1? tkVal: ArrayUtil.getNullIndex(aggTwo);
      Long[] tempMergeTopkArray = new Long[nullIndex1 + nullIndex2];

      for (int i = 0; i < nullIndex1; i++) {
        tempMergeTopkArray[i] = aggOne[i];
      }
      for (int i = nullIndex1; i < nullIndex1 + nullIndex2; i++) {
        tempMergeTopkArray[i] = aggTwo[i - nullIndex1];
      }
      Arrays.sort(tempMergeTopkArray, Collections.reverseOrder());
      if (tempMergeTopkArray.length < tkVal) {
        tempMergeTopkArray = ArrayUtil.padWithNull(Long.class, tempMergeTopkArray, tkVal);
        return tempMergeTopkArray;
      }
      return Arrays.copyOf(tempMergeTopkArray, tkVal);
    };
  }

  @Override
  public KsqlAggregateFunction<Long, Long[]> getInstance(Map<String, Integer> expressionNames,
                                                             List<Expression> functionArguments) {
    if (functionArguments.size() != 2) {
      throw new KsqlException(String.format("Invalid parameter count. Need 2 args, got %d arg(s)"
                                            + ".", functionArguments.size()));
    }
    int udafIndex = expressionNames.get(functionArguments.get(0).toString());
    Integer tkValFromArg = Integer.parseInt(functionArguments.get(1).toString());
    LongTopkKudaf longTopkKudaf = new LongTopkKudaf(udafIndex, tkValFromArg);
    return longTopkKudaf;
  }

}