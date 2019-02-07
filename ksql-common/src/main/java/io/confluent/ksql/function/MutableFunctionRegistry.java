/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function;

public interface MutableFunctionRegistry extends FunctionRegistry {

  /**
   * Ensure the supplied function factory is registered
   *
   * @param factory the factory to register.
   */
  void ensureFunctionFactory(UdfFactory factory);

  /**
   * Register the supplied {@code ksqlFunction}.
   *
   * <p>Note: a suitable function factory must already have been register via
   * {@link #ensureFunctionFactory(UdfFactory)}
   *
   * @param ksqlFunction the function to register.
   */
  void addFunction(KsqlFunction ksqlFunction);

  /**
   * Register an aggregate function factory.
   *
   * @param aggregateFunctionFactory the factory to register.
   */
  void addAggregateFunctionFactory(AggregateFunctionFactory aggregateFunctionFactory);
}
