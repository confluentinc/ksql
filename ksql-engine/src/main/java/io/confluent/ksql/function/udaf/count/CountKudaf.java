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

package io.confluent.ksql.function.udaf.count;

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.TableAggregationFunction;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

public class CountKudaf
    extends BaseAggregateFunction<Object, Long> implements TableAggregationFunction<Object, Long> {

  CountKudaf(final String functionName, final int argIndexInValue) {
    super(functionName, argIndexInValue, () -> 0L, Schema.OPTIONAL_INT64_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
        "Counts records by key."
    );
  }

  @Override
  public Long aggregate(final Object currentValue, final Long aggregateValue) {
    return aggregateValue + 1;
  }

  @Override
  public Merger<String, Long> getMerger() {
    return (aggKey, aggOne, aggTwo) -> aggOne + aggTwo;
  }

  @Override
  public Long undo(final Object valueToUndo, final Long aggregateValue) {
    return aggregateValue - 1;
  }

  @Override
  public KsqlAggregateFunction<Object, Long> getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    return new CountKudaf(functionName, aggregateFunctionArguments.udafIndex());
  }

  @Override
  public boolean hasSameArgTypes(final List<Schema> argTypeList) {
    return false;
  }
}
