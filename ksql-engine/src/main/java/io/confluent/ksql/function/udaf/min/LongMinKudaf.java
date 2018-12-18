/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.min;

import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

public class LongMinKudaf extends BaseAggregateFunction<Long, Long> {

  LongMinKudaf(final String functionName, final int argIndexInValue) {
    super(functionName, argIndexInValue, () -> Long.MAX_VALUE, Schema.OPTIONAL_INT64_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA),
        "Computes the minimum long value for a key."
    );
  }

  @Override
  public Long aggregate(final Long currentValue, final Long aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }
    return Math.min(currentValue, aggregateValue);
  }

  @Override
  public Merger<String, Long> getMerger() {
    return (aggKey, aggOne, aggTwo) -> Math.min(aggOne, aggTwo);
  }

  @Override
  public KsqlAggregateFunction<Long, Long> getInstance(
      final AggregateFunctionArguments aggregateFunctionArguments) {
    return new LongMinKudaf(functionName, aggregateFunctionArguments.udafIndex());
  }
}
