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

import org.apache.kafka.connect.data.Schema;

import java.util.Collections;
import java.util.List;

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.KsqlAggregateFunction;

public class CountAggFunctionFactory extends AggregateFunctionFactory {
  private static final String FUNCTION_NAME = "COUNT";

  public CountAggFunctionFactory() {
    super(FUNCTION_NAME, Collections.singletonList(new CountKudaf(FUNCTION_NAME, -1)));
  }

  @Override
  public KsqlAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    return getAggregateFunctionList().get(0);
  }
}
