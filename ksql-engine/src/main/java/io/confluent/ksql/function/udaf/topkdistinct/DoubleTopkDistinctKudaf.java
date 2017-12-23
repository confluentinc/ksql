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
import io.confluent.ksql.util.KsqlException;

public class DoubleTopkDistinctKudaf extends KsqlAggregateFunction<Double, Double[]> {

  Integer tkVal;
  Double[] topkArray;
  Double[] tempTopkArray;
  Double[] tempMergeTopkArray;

  public DoubleTopkDistinctKudaf(Integer argIndexInValue, Integer tkVal) {
    super(argIndexInValue, new Double[tkVal], SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(),
          Arrays.asList(Schema.FLOAT64_SCHEMA),
          "TOPKDISTINCT", DoubleTopkDistinctKudaf.class);
    this.tkVal = tkVal;
    this.topkArray = new Double[tkVal];
    this.tempTopkArray = new Double[tkVal + 1];
    this.tempMergeTopkArray = new Double[tkVal * 2];
  }

  @Override
  public Double[] aggregate(Double currentVal, Double[] currentAggVal) {
    if (currentVal == null) {
      return currentAggVal;
    }
    if (valueExists(currentVal, currentAggVal)) {
      return currentAggVal;
    }
    int nullIndex = getNullIndex(currentAggVal);
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
  public Merger<String, Double[]> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {
      for (int i = 0; i < tkVal; i++) {
        tempMergeTopkArray[i] = aggOne[i];
      }
      for (int i = tkVal; i < 2 * tkVal; i++) {
        tempMergeTopkArray[i] = aggTwo[i - tkVal];
      }
      Arrays.sort(tempMergeTopkArray, Collections.reverseOrder());
      return Arrays.copyOf(tempMergeTopkArray, tkVal);
    };
  }

  @Override
  public KsqlAggregateFunction<Double, Double[]> getInstance(Map<String, Integer> expressionNames,
                                                             List<Expression> functionArguments) {
    if (functionArguments.size() != 2) {
      throw new KsqlException(String.format("Invalid parameter count. Need 2 args, got %d arg(s)"
                                            + ".", functionArguments.size()));
    }
    int udafIndex = expressionNames.get(functionArguments.get(0).toString());
    Integer tkValFromArg = Integer.parseInt(functionArguments.get(1).toString());
    DoubleTopkDistinctKudaf doubleTopkDistinctKudaf = new DoubleTopkDistinctKudaf(udafIndex, tkValFromArg);
    return doubleTopkDistinctKudaf;
  }

  private boolean valueExists(Double value, Double[] valueArray) {
    for (Double d: valueArray) {
      if (d == value) {
        return true;
      }
    }
    return false;
  }

  private int getNullIndex(Double[] doubleArray) {
    for (int i = 0; i < doubleArray.length; i++) {
      if (doubleArray[i] == null) {
        return i;
      }
    }
    return -1;
  }

}