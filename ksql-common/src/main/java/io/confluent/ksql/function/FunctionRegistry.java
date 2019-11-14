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

import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.List;

public interface FunctionRegistry {

  SqlType DEFAULT_FUNCTION_ARG_SCHEMA = SqlTypes.BIGINT;

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
   * Test if the supplied {@code functionName} is a table function.
   *
   * <p>Note: unknown functions result in {@code false} return value.
   *
   * @param functionName the name of the function to test
   * @return {@code true} if it is a table function, {@code false} otherwise.
   */
  boolean isTableFunction(String functionName);

  /**
   * Get the factory for a UDF.
   *
   * @param functionName the name of the function.
   * @return the factory.
   * @throws KsqlException on unknown UDF.
   */
  UdfFactory getUdfFactory(String functionName);

  /**
   * Get the factory for a table function.
   *
   * @param functionName the name of the function.
   * @return the factory.
   * @throws KsqlException on unknown table function.
   */
  TableFunctionFactory getTableFunctionFactory(String functionName);

  /**
   * Get the factory for a UDAF.
   *
   * @param functionName the name of the function
   * @return the factory.
   * @throws KsqlException on unknown UDAF.
   */
  AggregateFunctionFactory getAggregateFactory(String functionName);

  /**
   * Get an instance of an aggregate function.
   *
   * <p>The current assumption is that all aggregate functions take a single argument for
   * computing the aggregate at runtime. For functions that have no runtime arguments pass {@link
   * #DEFAULT_FUNCTION_ARG_SCHEMA} for the {@code argumentType} parameter.
   *
   * <p>Some aggregate functions also take initialisation arguments, e.g.
   * <code> SELECT TOPK(AGE, 5) FROM PEOPLE</code>.
   *
   * <p>In the above 5 is an initialisation argument which needs to be provided when creating the
   * <code>KsqlAggregateFunction</code> instance.
   *
   * @param functionName the name of the function.
   * @param argumentType the schema of the argument.
   * @return the function instance.
   * @throws KsqlException on unknown UDAF, or on unsupported {@code argumentType}.
   */
  KsqlAggregateFunction<?, ?, ?> getAggregateFunction(
      String functionName,
      SqlType argumentType,
      AggregateFunctionInitArguments initArgs
  );

  /**
   * Get a table function.
   *
   * @param functionName the name of the function.
   * @param argumentTypes the schemas of the arguments.
   * @return the function instance.
   * @throws KsqlException on unknown table function, or on unsupported {@code argumentType}.
   */
  KsqlTableFunction getTableFunction(String functionName, List<SqlType> argumentTypes);

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
