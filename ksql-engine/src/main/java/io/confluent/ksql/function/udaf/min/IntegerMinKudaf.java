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
 */

package io.confluent.ksql.function.udaf.min;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.parser.tree.Expression;

public class IntegerMinKudaf extends KsqlAggregateFunction<Integer, Integer> {

  IntegerMinKudaf(int argIndexInValue) {
    super(argIndexInValue, () -> Integer.MAX_VALUE, Schema.INT32_SCHEMA,
          Collections.singletonList(Schema.INT32_SCHEMA)
    );
  }

  @Override
  public Integer aggregate(Integer currentVal, Integer currentAggVal) {
    if (currentVal == null) {
      return currentAggVal;
    }
    return Math.min(currentVal, currentAggVal);
  }

  @Override
  public Merger<String, Integer> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {
      return Math.min(aggOne, aggTwo);
    };
  }

  @Override
  public KsqlAggregateFunction<Integer, Integer> getInstance(Map<String, Integer> expressionNames,
                                                           List<Expression> functionArguments) {
    int udafIndex = expressionNames.get(functionArguments.get(0).toString());
    return new IntegerMinKudaf(udafIndex);
  }
}