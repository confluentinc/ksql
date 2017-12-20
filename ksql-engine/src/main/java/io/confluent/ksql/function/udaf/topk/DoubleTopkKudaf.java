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

import io.confluent.ksql.function.KsqlAggregateFunction;


public class DoubleTopkKudaf extends KsqlAggregateFunction<Double, Double[]> {

  Integer tkVal;
  Double[] topkArray;
  Double[] tempTopkArray;
  Double[] tempMergeTopkArray;

  public DoubleTopkKudaf(Integer argIndexInValue, Integer tkVal) {
    super(argIndexInValue, new Double[tkVal], SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(),
          Arrays.asList(Schema.FLOAT64_SCHEMA),
          "TOPK", DoubleTopkKudaf.class);
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
        tempMergeTopkArray[i] = aggTwo[i];
      }
      Arrays.sort(tempMergeTopkArray, Collections.reverseOrder());
      return Arrays.copyOf(tempMergeTopkArray, tkVal);
    };
  }
}