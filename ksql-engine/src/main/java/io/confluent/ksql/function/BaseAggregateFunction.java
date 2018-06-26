/**
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
 **/

package io.confluent.ksql.function;

import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.function.Supplier;

public abstract class BaseAggregateFunction<V, A> implements KsqlAggregateFunction<V, A> {
  /** An index of the function argument in the row that is used for computing the aggregate.
   * For instance, in the example SELECT key, SUM(ROWTIME), aggregation will be done on a row
   * with two columns (key, ROWTIME) and the `argIndexInValue` will be 1.
   **/
  private final int argIndexInValue;
  private final Supplier<A> initialValueSupplier;
  private final Schema returnType;
  private final List<Schema> arguments;

  protected final String functionName;
  private final String description;

  public BaseAggregateFunction(final String functionName, final Integer argIndexInValue) {
    this(functionName, argIndexInValue, null, null, null, "");
  }

  public BaseAggregateFunction(
      final String functionName,
      final int argIndexInValue,
      final Supplier<A> initialValueSupplier,
      final Schema returnType,
      final List<Schema> arguments,
      final String description
  ) {
    this.argIndexInValue = argIndexInValue;
    this.initialValueSupplier = initialValueSupplier;
    this.returnType = returnType;
    this.arguments = arguments;
    this.functionName = functionName;
    this.description = description;
  }

  public boolean hasSameArgTypes(List<Schema> argTypeList) {
    if (argTypeList == null) {
      throw new KsqlException("Argument type list is null.");
    }
    return this.arguments.equals(argTypeList);
  }

  public String getFunctionName() {
    return functionName;
  }

  public Supplier<A> getInitialValueSupplier() {
    return initialValueSupplier;
  }

  public int getArgIndexInValue() {
    return argIndexInValue;
  }

  public Schema getReturnType() {
    return returnType;
  }

  public List<Schema> getArgTypes() {
    return arguments;
  }

  @Override
  public String getDescription() {
    return description;
  }
}
