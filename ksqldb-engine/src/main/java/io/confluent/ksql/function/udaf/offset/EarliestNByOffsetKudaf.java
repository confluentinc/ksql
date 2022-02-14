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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;

public class EarliestNByOffsetKudaf extends
    BaseAggregateFunction<Object, List<Struct>, List<Object>> {

  private final boolean ignoreNulls;
  private final Schema internalStructSchema;
  private final int earliestN;

  public EarliestNByOffsetKudaf(
      final AggregateFunctionInitArguments initArgs,
      final SqlType aggregateType,
      final SqlType returnType,
      final boolean ignoreNulls,
      final int earliestN,
      final Schema internalStructSchema) {
    super(EarliestByOffsetFactory.FUNCTION_NAME,
        initArgs.udafIndex(),
        () -> new ArrayList<>(earliestN),
        aggregateType,
        returnType,
        Collections.singletonList(
            new ParameterInfo("key", ParamTypes.LONG, "", false)), "description");
    this.earliestN = earliestN;
    this.ignoreNulls = ignoreNulls;
    this.internalStructSchema = internalStructSchema;
  }

  @Override
  public List<Struct> aggregate(final Object currentValue, final List<Struct> aggregateValue) {
    if (currentValue == null && ignoreNulls) {
      return aggregateValue;
    }

    if (aggregateValue.size() < earliestN) {
      aggregateValue.add(
          KudafByOffsetUtils.createStruct(internalStructSchema, currentValue));
    }
    return aggregateValue;
  }

  @Override
  public Merger<GenericKey, List<Struct>> getMerger() {
    return (aggKey, aggOne, aggTwo) -> {
      final List<Struct> merged = new ArrayList<>(aggOne.size() + aggTwo.size());
      merged.addAll(aggOne);
      merged.addAll(aggTwo);
      merged.sort(INTERMEDIATE_STRUCT_COMPARATOR);
      return merged.subList(0, Math.min(earliestN, merged.size()));
    };
  }

  @Override
  public Function<List<Struct>, List<Object>> getResultMapper() {
    return agg -> agg.stream().map(s -> s.get(VAL_FIELD)).collect(Collectors.toList());
  }
}