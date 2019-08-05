/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.TableAggregationFunction;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;

public class LongSumKudaf
    extends BaseAggregateFunction<Long, Long> implements TableAggregationFunction<Long, Long> {

  LongSumKudaf(final String functionName, final int argIndexInValue) {
    super(functionName, argIndexInValue, () -> 0L, Schema.OPTIONAL_INT64_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA),
        "Computes the sum for a key.");
  }

  @Override
  public Long aggregate(final Long valueToAdd, final Long aggregateValue) {
    if (valueToAdd == null) {
      return aggregateValue;
    }
    return aggregateValue + valueToAdd;
  }

  @Override
  public Long undo(final Long valueToUndo, final Long aggregateValue) {
    if (valueToUndo == null) {
      return aggregateValue;
    }
    return aggregateValue - valueToUndo;
  }

  @Override
  public Merger<Struct, Long> getMerger() {
    return (aggKey, aggOne, aggTwo) -> aggOne + aggTwo;
  }

  @Override
  public KsqlAggregateFunction<Long, Long> getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    return new LongSumKudaf(functionName, aggregateFunctionArguments.udafIndex());
  }
}
