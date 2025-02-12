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
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.ParserKeywordValidatorUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class InternalFunctionRegistry implements MutableFunctionRegistry {

  private final Map<String, UdfFactory> udfs = new HashMap<>();
  private final Map<String, AggregateFunctionFactory> udafs = new HashMap<>();
  private final Map<String, TableFunctionFactory> udtfs = new HashMap<>();
  private final ParserKeywordValidatorUtil functionNameValidator = new ParserKeywordValidatorUtil();

  @Override
  public synchronized UdfFactory getUdfFactory(final FunctionName functionName) {
    final UdfFactory udfFactory = udfs.get(functionName.text().toUpperCase());
    if (udfFactory == null) {
      throw new KsqlException(
          "Can't find any functions with the name '" + functionName.text() + "'");
    }
    return udfFactory;
  }

  @Override
  public synchronized void addFunction(final KsqlScalarFunction ksqlFunction) {
    final UdfFactory udfFactory = udfs.get(ksqlFunction.name().text().toUpperCase());
    if (udfFactory == null) {
      throw new KsqlException("Unknown function factory: " + ksqlFunction.name());
    }
    udfFactory.addFunction(ksqlFunction);
  }

  @Override
  public synchronized UdfFactory ensureFunctionFactory(final UdfFactory factory) {
    validateFunctionName(factory.getName());

    final String functionName = factory.getName().toUpperCase();
    if (udafs.containsKey(functionName)) {
      throw new KsqlException("UdfFactory already registered as aggregate: " + functionName);
    }
    if (udtfs.containsKey(functionName)) {
      throw new KsqlException("UdfFactory already registered as table function: " + functionName);
    }

    final UdfFactory existing = udfs.putIfAbsent(functionName, factory);
    if (existing != null && !existing.matches(factory)) {
      throw new KsqlException("UdfFactory not compatible with existing factory."
          + " function: " + functionName
          + " existing: " + existing
          + ", factory: " + factory);
    }

    return existing == null ? factory : existing;
  }

  @Override
  public synchronized boolean isAggregate(final FunctionName functionName) {
    return udafs.containsKey(functionName.text().toUpperCase());
  }

  @Override
  public synchronized boolean isTableFunction(final FunctionName functionName) {
    return udtfs.containsKey(functionName.text().toUpperCase());
  }

  @Override
  public boolean isPresent(final FunctionName functionName) {
    if (udfs.containsKey(functionName.text().toUpperCase())) {
      return true;
    }
    if (udafs.containsKey(functionName.text().toUpperCase())) {
      return true;
    }
    return udtfs.containsKey(functionName.text().toUpperCase());
  }

  @Override
  public synchronized KsqlTableFunction getTableFunction(
      final FunctionName functionName,
      final List<SqlArgument> argumentTypes
  ) {
    final TableFunctionFactory udtfFactory = udtfs.get(functionName.text().toUpperCase());
    if (udtfFactory == null) {
      throw new KsqlException("No table function with name " + functionName + " exists!");
    }

    return udtfFactory.createTableFunction(argumentTypes);
  }

  @Override
  public synchronized void addAggregateFunctionFactory(
      final AggregateFunctionFactory aggregateFunctionFactory) {
    final String functionName = aggregateFunctionFactory.getName().toUpperCase();
    validateFunctionName(functionName);

    if (udfs.containsKey(functionName)) {
      throw new KsqlException(
          "Aggregate function already registered as non-aggregate: " + functionName);
    }

    if (udtfs.containsKey(functionName)) {
      throw new KsqlException(
          "Aggregate function already registered as table function: " + functionName);
    }

    if (udafs.putIfAbsent(functionName, aggregateFunctionFactory) != null) {
      throw new KsqlException("Aggregate function already registered: " + functionName);
    }

  }

  @Override
  public synchronized void addTableFunctionFactory(
      final TableFunctionFactory tableFunctionFactory) {
    final String functionName = tableFunctionFactory.getName().toUpperCase();
    validateFunctionName(functionName);

    if (udfs.containsKey(functionName)) {
      throw new KsqlException(
          "Table function already registered as non-aggregate: " + functionName);
    }

    if (udafs.containsKey(functionName)) {
      throw new KsqlException(
          "Table function already registered as aggregate: " + functionName);
    }

    if (udtfs.putIfAbsent(functionName, tableFunctionFactory) != null) {
      throw new KsqlException("Table function already registered: " + functionName);
    }

  }

  @Override
  public synchronized List<UdfFactory> listFunctions() {
    return new ArrayList<>(udfs.values());
  }

  @Override
  public synchronized AggregateFunctionFactory getAggregateFactory(
      final FunctionName functionName) {
    final AggregateFunctionFactory udafFactory = udafs.get(functionName.text().toUpperCase());
    if (udafFactory == null) {
      throw new KsqlException(
          "Can not find any aggregate functions with the name '" + functionName + "'");
    }

    return udafFactory;
  }

  @Override
  public synchronized TableFunctionFactory getTableFunctionFactory(
      final FunctionName functionName) {
    final TableFunctionFactory tableFunctionFactory = udtfs.get(functionName.text().toUpperCase());
    if (tableFunctionFactory == null) {
      throw new KsqlException(
          "Can not find any table functions with the name '" + functionName + "'");
    }
    return tableFunctionFactory;
  }

  @Override
  public synchronized List<AggregateFunctionFactory> listAggregateFunctions() {
    return new ArrayList<>(udafs.values());
  }

  @Override
  public synchronized List<TableFunctionFactory> listTableFunctions() {
    return new ArrayList<>(udtfs.values());
  }

  private void validateFunctionName(final String functionName) {
    if (!functionNameValidator.test(functionName)) {
      throw new KsqlException(functionName + " is not a valid function name."
          + " Function names must be valid java identifiers and not a KSQL reserved word"
      );
    }
  }
}
