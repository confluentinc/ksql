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

package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.TableAggregationFunction;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

import java.util.Collections;

import io.confluent.ksql.function.KsqlAggregateFunction;

public class LongSumKudaf
    extends BaseAggregateFunction<Long, Long> implements TableAggregationFunction<Long, Long> {

  LongSumKudaf(String functionName, int argIndexInValue) {
    super(functionName, argIndexInValue, () -> 0L, Schema.OPTIONAL_INT64_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA),
        "Computes the sum for a key.");
  }

  @Override
  public Long aggregate(Long currentValue, Long aggregateValue) {
    return currentValue + aggregateValue;
  }

  @Override
  public Merger<String, Long> getMerger() {
    return (aggKey, aggOne, aggTwo) -> aggOne + aggTwo;
  }

  @Override
  public Long undo(Long valueToUndo, Long aggregateValue) {
    return aggregateValue - valueToUndo;
  }

  @Override
  public KsqlAggregateFunction<Long, Long> getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    return new LongSumKudaf(functionName, aggregateFunctionArguments.udafIndex());
  }
}
