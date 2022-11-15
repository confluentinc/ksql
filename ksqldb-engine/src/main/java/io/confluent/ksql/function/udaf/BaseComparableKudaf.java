/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.ParameterInfo;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Collections;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.streams.kstream.Merger;

public abstract class BaseComparableKudaf<T extends Comparable<? super T>> extends
    BaseAggregateFunction<T, T, T> {

  private final BiFunction<T, T, T> aggregatePrimitive;

  public BaseComparableKudaf(
      final String functionName,
      final Integer argIndexInValue,
      final SqlType outputSchema,
      final BiFunction<T, T, T> aggregatePrimitive,
      final String description
  ) {
    super(functionName,
        argIndexInValue,
        () -> null,
        outputSchema,
        outputSchema,
        Collections.singletonList(
            new ParameterInfo(
                "value",
                SchemaConverters.sqlToFunctionConverter().toFunctionType(outputSchema),
                "the value to aggregate",
                false)
            ),
        description);
    this.aggregatePrimitive = Objects.requireNonNull(aggregatePrimitive, "aggregatePrimitive");
  }

  @Override
  public final T aggregate(final T currentValue, final T aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }

    if (aggregateValue == null) {
      return currentValue;
    }

    return aggregatePrimitive.apply(aggregateValue, currentValue);
  }

  @Override
  public final Merger<GenericKey, T> getMerger() {
    return (key, a, b) -> aggregate(a, b);
  }

  @Override
  public Function<T, T> getResultMapper() {
    return Function.identity();
  }
}
