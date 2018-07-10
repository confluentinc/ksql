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

package io.confluent.ksql.function.udaf.topkdistinct;

import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.KsqlException;

public class TopkDistinctAggFunctionFactory extends AggregateFunctionFactory {

  private static final String NAME = "TOPKDISTINCT";
  private final Map<Schema.Type, KsqlAggregateFunction<?, ?>> functions = new HashMap<>();

  public TopkDistinctAggFunctionFactory() {
    super(NAME, createDescriptionFunctions());
    eachFunction(func -> {
      functions.put(((TopkDistinctKudaf)func).getOutputSchema().type(), func);
    });
  }

  private static List<KsqlAggregateFunction<?, ?>> createDescriptionFunctions() {
    return Arrays.asList(
        new TopkDistinctKudaf<>(NAME, -1, 0, Schema.OPTIONAL_INT32_SCHEMA,
            Integer.class),
        new TopkDistinctKudaf<>(NAME, -1, 0, Schema.OPTIONAL_INT64_SCHEMA,
            Long.class),
        new TopkDistinctKudaf<>(NAME, -1, 0, Schema.OPTIONAL_FLOAT64_SCHEMA,
            Double.class),
        new TopkDistinctKudaf<>(NAME, -1, 0, Schema.OPTIONAL_STRING_SCHEMA,
            String.class)
    );
  }

  @Override
  public KsqlAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    if (argTypeList.isEmpty()) {
      throw new KsqlException("TOPKDISTINCT function should have two arguments.");
    }

    final KsqlAggregateFunction<?, ?> function = functions.get(argTypeList.get(0).type());
    if (function == null) {
      throw new KsqlException("No TOPKDISTINCT aggregate function with " + argTypeList.get(0)
          + " argument type exists!");
    }
    return function;
  }
}