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
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public interface FunctionRegistry {

  Schema DEFAULT_FUNCTION_ARG_SCHEMA = Schema.OPTIONAL_INT64_SCHEMA;

  /**
   * Test if the supplied {@code functionName} is an aggregate function.
   *
   * <p>Note: unknown functions result in {@code false} return value.
   *
   * @param functionName the name of the function to test
   * @return {@code true} if it is an aggregate function, {@code false} otherwise.
   */
  boolean isAggregate(String functionName);

  /**
   * Get the factory for a UDF.
   *
   * @param functionName the name of the function.
   * @return the factory.
   * @throws KsqlException on unknown UDF.
   */
  UdfFactory getUdfFactory(String functionName);

  /**
   * Get the factory for a UDAF.
   * @param functionName the name of the function
   * @return the factory.
   * @throws KsqlException on unknown UDAF.
   */
  AggregateFunctionFactory getAggregateFactory(String functionName);

  /**
   * Get an instance of an aggregate function.
   *
   * <p>The current assumption is that all aggregate functions take a single argument.
   * For functions that have no arguments pass {@link #DEFAULT_FUNCTION_ARG_SCHEMA} for the
   * {@code argumentType} parameter.
   *
   * @param functionName the name of the function.
   * @param argumentType the schema of the argument or {@link #DEFAULT_FUNCTION_ARG_SCHEMA}.
   * @return the function instance.
   * @throws KsqlException on unknown UDAF, or on unsupported {@code argumentType}.
   */
  KsqlAggregateFunction<?, ?> getAggregate(String functionName, Schema argumentType);

  /**
   * @return all UDF factories.
   */
  List<UdfFactory> listFunctions();

  /**
   * @return all UDAF factories.
   */
  List<AggregateFunctionFactory> listAggregateFunctions();
}
