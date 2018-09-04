/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

public class UdafAggregateFunctionFactory extends AggregateFunctionFactory {
  private final Map<List<Schema>, KsqlAggregateFunction<?, ?>> aggregateFunctions = new HashMap<>();

  @SuppressWarnings("unchecked")
  public UdafAggregateFunctionFactory(final UdfMetadata metadata,
                                      final List<KsqlAggregateFunction<?, ?>> functionList) {
    super(metadata, functionList);
    functionList
        .forEach(function -> aggregateFunctions.put(function.getArgTypes(), function));
  }

  @Override
  public KsqlAggregateFunction<?, ?> getProperAggregateFunction(final List<Schema> argTypeList) {
    final KsqlAggregateFunction ksqlAggregateFunction = aggregateFunctions.get(argTypeList);
    if (ksqlAggregateFunction == null) {
      throw new KsqlException("There is no aggregate function with name='" + getName()
          + "' that has arguments of type="
          + argTypeList.stream().map(schema -> schema.type().getName())
          .collect(Collectors.joining(",")));
    }
    return ksqlAggregateFunction;
  }
}
