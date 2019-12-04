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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udaf.count.CountAggFunctionFactory;
import io.confluent.ksql.function.udaf.max.MaxAggFunctionFactory;
import io.confluent.ksql.function.udaf.min.MinAggFunctionFactory;
import io.confluent.ksql.function.udaf.sum.SumAggFunctionFactory;
import io.confluent.ksql.function.udaf.topk.TopKAggregateFunctionFactory;
import io.confluent.ksql.function.udaf.topkdistinct.TopkDistinctAggFunctionFactory;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.function.udf.json.ArrayContainsKudf;
import io.confluent.ksql.function.udf.json.JsonExtractStringKudf;
import io.confluent.ksql.function.udf.math.RandomKudf;
import io.confluent.ksql.function.udf.string.ConcatKudf;
import io.confluent.ksql.function.udf.string.IfNullKudf;
import io.confluent.ksql.function.udf.string.LCaseKudf;
import io.confluent.ksql.function.udf.string.LenKudf;
import io.confluent.ksql.function.udf.string.TrimKudf;
import io.confluent.ksql.function.udf.string.UCaseKudf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class InternalFunctionRegistry implements MutableFunctionRegistry {

  private final Map<String, UdfFactory> udfs = new HashMap<>();
  private final Map<String, AggregateFunctionFactory> udafs = new HashMap<>();
  private final Map<String, TableFunctionFactory> udtfs = new HashMap<>();
  private final FunctionNameValidator functionNameValidator = new FunctionNameValidator();

  public InternalFunctionRegistry() {
    new BuiltInInitializer(this).init();
  }

  @Override
  public synchronized UdfFactory getUdfFactory(final String functionName) {
    final UdfFactory udfFactory = udfs.get(functionName.toUpperCase());
    if (udfFactory == null) {
      throw new KsqlException("Can't find any functions with the name '" + functionName + "'");
    }
    return udfFactory;
  }

  @Override
  public synchronized void addFunction(final KsqlScalarFunction ksqlFunction) {
    final UdfFactory udfFactory = udfs.get(ksqlFunction.name().name().toUpperCase());
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
  public synchronized boolean isAggregate(final String functionName) {
    return udafs.containsKey(functionName.toUpperCase());
  }

  @Override
  public synchronized boolean isTableFunction(final String functionName) {
    return udtfs.containsKey(functionName.toUpperCase());
  }

  @Override
  public synchronized KsqlAggregateFunction getAggregateFunction(
      final String functionName,
      final SqlType argumentType,
      final AggregateFunctionInitArguments initArgs
  ) {
    final AggregateFunctionFactory udafFactory = udafs.get(functionName.toUpperCase());
    if (udafFactory == null) {
      throw new KsqlException("No aggregate function with name " + functionName + " exists!");
    }
    return udafFactory.createAggregateFunction(
        Collections.singletonList(argumentType),
        initArgs
    );
  }

  @Override
  public synchronized KsqlTableFunction getTableFunction(
      final String functionName,
      final List<SqlType> argumentTypes
  ) {
    final TableFunctionFactory udtfFactory = udtfs.get(functionName.toUpperCase());
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
  public synchronized AggregateFunctionFactory getAggregateFactory(final String functionName) {
    final AggregateFunctionFactory udafFactory = udafs.get(functionName.toUpperCase());
    if (udafFactory == null) {
      throw new KsqlException(
          "Can not find any aggregate functions with the name '" + functionName + "'");
    }

    return udafFactory;
  }

  @Override
  public synchronized TableFunctionFactory getTableFunctionFactory(final String functionName) {
    final TableFunctionFactory tableFunctionFactory = udtfs.get(functionName.toUpperCase());
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

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private static final class BuiltInInitializer {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

    private final InternalFunctionRegistry functionRegistry;

    private BuiltInInitializer(
        final InternalFunctionRegistry functionRegistry
    ) {
      this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    }

    private static UdfFactory builtInUdfFactory(
        final KsqlScalarFunction ksqlFunction
    ) {
      final UdfMetadata metadata = new UdfMetadata(
          ksqlFunction.name().name(),
          ksqlFunction.getDescription(),
          KsqlConstants.CONFLUENT_AUTHOR,
          "",
          KsqlScalarFunction.INTERNAL_PATH
      );

      return new UdfFactory(ksqlFunction.getKudfClass(), metadata);
    }

    private void init() {
      addStringFunctions();
      addMathFunctions();
      addJsonFunctions();
      addUdafFunctions();
    }

    private void addStringFunctions() {

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.STRING,
          Collections.singletonList(ParamTypes.STRING),
          FunctionName.of("LCASE"), LCaseKudf.class
      ));

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.STRING,
          Collections.singletonList(ParamTypes.STRING),
          FunctionName.of("UCASE"), UCaseKudf.class
      ));

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.STRING,
          ImmutableList.of(ParamTypes.STRING, ParamTypes.STRING),
          FunctionName.of(ConcatKudf.NAME), ConcatKudf.class
      ));

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.STRING,
          Collections.singletonList(ParamTypes.STRING),
          FunctionName.of("TRIM"), TrimKudf.class
      ));

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.STRING,
          ImmutableList.of(
              ParamTypes.STRING,
              ParamTypes.STRING
          ),
          FunctionName.of("IFNULL"), IfNullKudf.class
      ));

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.INTEGER,
          Collections.singletonList(ParamTypes.STRING),
          FunctionName.of("LEN"),
          LenKudf.class
      ));
    }

    private void addMathFunctions() {
      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.DOUBLE,
          Collections.emptyList(),
          FunctionName.of("RANDOM"),
          RandomKudf.class
      ));
    }

    private void addJsonFunctions() {

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.STRING,
          ImmutableList.of(ParamTypes.STRING, ParamTypes.STRING),
          JsonExtractStringKudf.FUNCTION_NAME,
          JsonExtractStringKudf.class
      ));

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.BOOLEAN,
          ImmutableList.of(ParamTypes.STRING, ParamTypes.STRING),
          FunctionName.of("ARRAYCONTAINS"),
          ArrayContainsKudf.class
      ));

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.BOOLEAN,
          ImmutableList.of(
              ArrayType.of(ParamTypes.STRING),
              ParamTypes.STRING
          ),
          FunctionName.of("ARRAYCONTAINS"),
          ArrayContainsKudf.class
      ));

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.BOOLEAN,
          ImmutableList.of(
              ArrayType.of(ParamTypes.INTEGER),
              ParamTypes.INTEGER
          ),
          FunctionName.of("ARRAYCONTAINS"),
          ArrayContainsKudf.class
      ));

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.BOOLEAN,
          ImmutableList.of(
              ArrayType.of(ParamTypes.LONG),
              ParamTypes.LONG
          ),
          FunctionName.of("ARRAYCONTAINS"),
          ArrayContainsKudf.class
      ));

      addBuiltInFunction(KsqlScalarFunction.createLegacyBuiltIn(
          SqlTypes.BOOLEAN,
          ImmutableList.of(
              ArrayType.of(ParamTypes.DOUBLE),
              ParamTypes.DOUBLE
          ),
          FunctionName.of("ARRAYCONTAINS"),
          ArrayContainsKudf.class
      ));
    }

    private void addUdafFunctions() {

      functionRegistry.addAggregateFunctionFactory(new CountAggFunctionFactory());
      functionRegistry.addAggregateFunctionFactory(new SumAggFunctionFactory());

      functionRegistry.addAggregateFunctionFactory(new MaxAggFunctionFactory());
      functionRegistry.addAggregateFunctionFactory(new MinAggFunctionFactory());

      functionRegistry.addAggregateFunctionFactory(new TopKAggregateFunctionFactory());
      functionRegistry.addAggregateFunctionFactory(new TopkDistinctAggFunctionFactory());
    }

    private void addBuiltInFunction(final KsqlScalarFunction ksqlFunction) {
      functionRegistry
          .ensureFunctionFactory(builtInUdfFactory(ksqlFunction))
          .addFunction(ksqlFunction);
    }
  }
}
