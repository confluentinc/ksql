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

package io.confluent.ksql.function;

import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

public class UdafAggregateFunctionFactory extends AggregateFunctionFactory {

  private final UdfIndex<UdafFactoryInvoker> udfIndex;

  UdafAggregateFunctionFactory(
      final UdfMetadata metadata,
      final List<UdafFactoryInvoker> factoryList
  ) {
    super(metadata);
    udfIndex = new UdfIndex<>(metadata.getName());
    factoryList.forEach(udfIndex::addFunction);
  }

  @Override
  public synchronized KsqlAggregateFunction<?, ?, ?> createAggregateFunction(
      final List<Schema> argTypeList,
      final AggregateFunctionInitArguments initArgs
  ) {
    final UdafFactoryInvoker creator = udfIndex.getFunction(argTypeList);
    if (creator == null) {
      throw new KsqlException("There is no aggregate function with name='" + getName()
          + "' that has arguments of type="
          + argTypeList.stream().map(schema -> schema.type().getName())
          .collect(Collectors.joining(",")));
    }
    return creator.createFunction(initArgs);
  }

  @Override
  public synchronized List<List<Schema>> supportedArgs() {
    return udfIndex.values()
        .stream()
        .map(UdafFactoryInvoker::getArguments)
        .collect(Collectors.toList());
  }
}
