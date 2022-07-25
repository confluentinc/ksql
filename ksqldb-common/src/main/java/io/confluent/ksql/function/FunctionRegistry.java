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

import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.KsqlException;
import java.util.List;

@EffectivelyImmutable
public interface FunctionRegistry {

  SqlType DEFAULT_FUNCTION_ARG_SCHEMA = SqlTypes.BIGINT;

  /**
   * Test if there is an aggregate function with the supplied {@code functionName}.
   *
   * <p>Note: unknown functions result in {@code false} return value.
   *
   * @param functionName the name of the function to test
   * @return {@code true} if it is an aggregate function, {@code false} otherwise.
   */
  boolean isAggregate(FunctionName functionName);

  /**
   * Test if there is a table function with the supplied {@code functionName}.
   *
   * <p>Note: unknown functions result in {@code false} return value.
   *
   * @param functionName the name of the function to test
   * @return {@code true} if it is a table function, {@code false} otherwise.
   */
  boolean isTableFunction(FunctionName functionName);

  /**
   * Test whether there is a function with the supplied {@code functionName}.
   *
   * @param functionName the name of the function to test
   * @return {@code true} if the function exists, {@code false} otherwise.
   */
  boolean isPresent(FunctionName functionName);

  /**
   * Get the factory for a UDF.
   *
   * @param functionName the name of the function.
   * @return the factory.
   * @throws KsqlException on unknown UDF.
   */
  UdfFactory getUdfFactory(FunctionName functionName);

  /**
   * Get the factory for a table function.
   *
   * @param functionName the name of the function.
   * @return the factory.
   * @throws KsqlException on unknown table function.
   */
  TableFunctionFactory getTableFunctionFactory(FunctionName functionName);

  /**
   * Get the factory for a UDAF.
   *
   * @param functionName the name of the function
   * @return the factory.
   * @throws KsqlException on unknown UDAF.
   */
  AggregateFunctionFactory getAggregateFactory(FunctionName functionName);

  /**
   * Get a table function.
   *
   * @param functionName  the name of the function.
   * @param argumentTypes the schemas of the arguments.
   * @return the function instance.
   * @throws KsqlException on unknown table function, or on unsupported {@code argumentType}.
   */
  KsqlTableFunction getTableFunction(FunctionName functionName, List<SqlArgument> argumentTypes);

  /**
   * @return all UDF factories.
   */
  List<UdfFactory> listFunctions();

  /**
   * @return all table function factories.
   */
  List<TableFunctionFactory> listTableFunctions();

  /**
   * @return all UDAF factories.
   */
  List<AggregateFunctionFactory> listAggregateFunctions();
}
