/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udaf.offset;

import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.INTERMEDIATE_STRUCT_COMPARATOR;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.VAL_FIELD;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.BaseAggregateFunction;
import io.confluent.ksql.function.ParameterInfo;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Collections;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;

public class EarliestByOffsetKudaf extends BaseAggregateFunction<Object, Struct, Object> {

  private final boolean ignoreNulls;
  private final Schema internalStructSchema;

  public EarliestByOffsetKudaf(
      final AggregateFunctionInitArguments initArgs,
      final SqlType aggregateType,
      final SqlType returnType,
      final boolean ignoreNulls,
      final Schema internalStructSchema) {
    super(EarliestByOffsetFactory.FUNCTION_NAME,
        initArgs.udafIndex(),
        () -> null,
        aggregateType,
        returnType,
        Collections.singletonList(
            new ParameterInfo("key", ParamTypes.LONG, "", false)), "description");
    this.ignoreNulls = ignoreNulls;
    this.internalStructSchema = internalStructSchema;
  }

  @Override
  public Struct aggregate(final Object currentValue, final Struct aggregateValue) {
    if (aggregateValue != null) {
      return aggregateValue;
    }

    if (currentValue == null && ignoreNulls) {
      return null;
    }
    return
        KudafByOffsetUtils.createStruct(internalStructSchema, currentValue);
  }

  @Override
  public Merger<GenericKey, Struct> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {
      if (aggOne == null) {
        return aggTwo;
      }

      if (aggTwo == null) {
        return aggOne;
      }

      // When merging we need some way of evaluating the "earliest" one.
      // We do this by keeping track of the sequence of when it was originally processed
      if (INTERMEDIATE_STRUCT_COMPARATOR.compare(aggOne, aggTwo) < 0) {
        return aggOne;
      } else {
        return aggTwo;
      }
    };
  }

  @Override
  public Function<Struct, Object> getResultMapper() {
    return struct -> {
      if (struct == null) {
        return null;
      }
      return struct.get(VAL_FIELD);
    };
  }
}