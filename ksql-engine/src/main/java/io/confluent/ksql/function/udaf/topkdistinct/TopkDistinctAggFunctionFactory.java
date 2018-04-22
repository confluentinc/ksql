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
import java.util.List;

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.KsqlException;

public class TopkDistinctAggFunctionFactory extends AggregateFunctionFactory {

  public TopkDistinctAggFunctionFactory() {
    super("TOPKDISTINCT", Arrays.asList());
  }

  @Override
  public KsqlAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    if (argTypeList.isEmpty()) {
      throw new KsqlException("TOPKDISTINCT function should have two arguments.");
    }
    Schema argSchema = argTypeList.get(0);
    switch (argSchema.type()) {
      case INT32:
        return new TopkDistinctKudaf<>(-1, 0, Schema.INT32_SCHEMA, Integer.class);
      case INT64:
        return new TopkDistinctKudaf<>(-1, 0, Schema.INT64_SCHEMA, Long.class);
      case FLOAT64:
        return new TopkDistinctKudaf<>(-1, 0, Schema.FLOAT64_SCHEMA, Double.class);
      case STRING:
        return new TopkDistinctKudaf<>(-1, 0, Schema.STRING_SCHEMA, String.class);
      default:
        throw new KsqlException("No TOPKDISTINCT aggregate function with " + argTypeList.get(0)
                                + " argument type exists!");
    }

  }
}
