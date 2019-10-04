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

import io.confluent.ksql.function.udaf.UdafArgApplier;
import io.confluent.ksql.name.FunctionName;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.data.Schema;

class UdafCreator implements IndexableAsFunction {

  private final FunctionName functionName;
  private final UdafArgApplier argSupplier;
  private final Schema aggregateArgType;
  private final Schema aggregateReturnType;
  private final Optional<Metrics> metrics;
  private final List<Schema> argTypes;

  UdafCreator(
      final String functionName,
      final UdafArgApplier argSupplier,
      final Schema aggregateArgType,
      final Schema aggregateReturnType,
      final Optional<Metrics> metrics,
      final List<Schema> argTypes
  ) {
    this.functionName = FunctionName.of(functionName);
    this.argSupplier = argSupplier;
    this.aggregateArgType = aggregateArgType;
    this.aggregateReturnType = aggregateReturnType;
    this.metrics = metrics;
    this.argTypes = argTypes;
  }

  KsqlAggregateFunction createFunction(final AggregateFunctionInitArguments initArgs) {
    return argSupplier.apply(initArgs, argTypes, aggregateArgType, aggregateReturnType, metrics);
  }

  @Override
  public FunctionName getFunctionName() {
    return functionName;
  }

  @Override
  public List<Schema> getArguments() {
    return argTypes;
  }

  @Override
  public boolean isVariadic() {
    return false;
  }
}
