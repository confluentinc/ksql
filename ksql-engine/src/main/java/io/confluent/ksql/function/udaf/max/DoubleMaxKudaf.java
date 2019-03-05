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

package io.confluent.ksql.function.udaf.max;

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

public class DoubleMaxKudaf extends BaseAggregateFunction<Double, Double> {

  DoubleMaxKudaf(final String functionName, final int argIndexInValue) {
    super(functionName, argIndexInValue, () -> Double.NEGATIVE_INFINITY,
        Schema.OPTIONAL_FLOAT64_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
        "Computes the maximum double value for a key."
    );
  }

  @Override
  public Double aggregate(final Double currentValue, final Double aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }
    return Math.max(currentValue, aggregateValue);
  }

  @Override
  public Merger<String, Double> getMerger() {
    return (aggKey, aggOne, aggTwo) -> Math.max(aggOne, aggTwo);
  }

  @Override
  public KsqlAggregateFunction<Double, Double> getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    return new DoubleMaxKudaf(functionName, aggregateFunctionArguments.udafIndex());
  }
}
