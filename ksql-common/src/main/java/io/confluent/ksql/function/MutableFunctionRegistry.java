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

import io.confluent.ksql.util.KsqlException;

public interface MutableFunctionRegistry extends FunctionRegistry {

  /**
   * Copy this function registry. This is useful for creating multiple
   * function registries across different execution contexts
   *
   * @return FunctionRegistry A copy of the current function registry
   */
  MutableFunctionRegistry copy();

  /**
   * Ensure the supplied function factory is registered.
   *
   * <p>The method will register the factory if a factory with the same name is not already
   * registered. If a factory with the same name is already registered the method will throw
   * if the two factories not are equivalent, (see {@link UdfFactory#matches(UdfFactory)}.
   *
   * @param factory the factory to register.
   * @return the udf factory.
   * @throws KsqlException if a UDAF function with the same name exists, or if an incompatible UDF
   *     function factory already exists.
   */
  UdfFactory ensureFunctionFactory(UdfFactory factory);

  /**
   * Register the supplied {@code ksqlFunction}.
   *
   * <p>Note: a suitable function factory must already have been registered via
   * {@link #ensureFunctionFactory(UdfFactory)}.
   *
   * @param ksqlFunction the function to register.
   * @throws KsqlException if a function, (of any type), with the same name exists.
   */
  void addFunction(KsqlFunction ksqlFunction);

  /**
   * Register the supplied {@code ksqlFunction}. If the function exists, replace it.
   *
   * <p>Note: a suitable function factory must already have been registered via
   * {@link #ensureFunctionFactory(UdfFactory)}.
   *
   * @param ksqlFunction the function to register.
   */
  void addOrReplaceFunction(KsqlFunction ksqlFunction);

  /**
   * Register an aggregate function factory.
   *
   * @param aggregateFunctionFactory the factory to register.
   * @throws KsqlException if a function, (of any type), with the same name exists.
   */
  void addAggregateFunctionFactory(AggregateFunctionFactory aggregateFunctionFactory);

  /**
   * Drop the specified function
   *
   * <p>Note: a suitable function factory must already have been registered via
   * {@link #ensureFunctionFactory(UdfFactory)}.
   *
   * @param ksqlFunctionName the name of the function to drop
   * @throws KsqlException if a function with the same name does not exist.
   */
  void dropFunction(String ksqlFunctionName);
}
