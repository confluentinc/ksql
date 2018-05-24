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

package io.confluent.ksql.function;

import org.apache.kafka.connect.data.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestFunctionRegistry implements FunctionRegistry {
  private final Map<String, KsqlFunction> udfs = new HashMap<>();
  private final Map<String, AggregateFunctionFactory> udafs = new HashMap<>();

  @Override
  public KsqlFunction getFunction(String functionName) {
    return udfs.get(functionName);
  }

  @Override
  public boolean addFunction(KsqlFunction ksqlFunction) {
    return udfs.putIfAbsent(ksqlFunction.getFunctionName().toUpperCase(), ksqlFunction) == null;

  }

  @Override
  public boolean isAggregate(String functionName) {
    return udafs.containsKey(functionName.toUpperCase());
  }

  @Override
  public KsqlAggregateFunction getAggregate(String functionName,
                                            Schema expressionType) {
    return udafs.get(functionName.toUpperCase()).getProperAggregateFunction(
        Collections.singletonList(expressionType));
  }

  @Override
  public void addAggregateFunctionFactory(AggregateFunctionFactory aggregateFunctionFactory) {
    udafs.put(aggregateFunctionFactory.functionName.toUpperCase(), aggregateFunctionFactory);
  }

  @Override
  public FunctionRegistry copy() {
    return this;
  }
}
