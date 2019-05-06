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
import io.confluent.ksql.function.udaf.count.CountAggFunctionFactory;
import io.confluent.ksql.function.udaf.max.MaxAggFunctionFactory;
import io.confluent.ksql.function.udaf.min.MinAggFunctionFactory;
import io.confluent.ksql.function.udaf.sum.SumAggFunctionFactory;
import io.confluent.ksql.function.udaf.topk.TopKAggregateFunctionFactory;
import io.confluent.ksql.function.udaf.topkdistinct.TopkDistinctAggFunctionFactory;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.function.udf.geo.GeoDistanceKudf;
import io.confluent.ksql.function.udf.json.ArrayContainsKudf;
import io.confluent.ksql.function.udf.json.JsonExtractStringKudf;
import io.confluent.ksql.function.udf.math.AbsKudf;
import io.confluent.ksql.function.udf.math.CeilKudf;
import io.confluent.ksql.function.udf.math.FloorKudf;
import io.confluent.ksql.function.udf.math.RandomKudf;
import io.confluent.ksql.function.udf.math.RoundKudf;
import io.confluent.ksql.function.udf.string.ConcatKudf;
import io.confluent.ksql.function.udf.string.IfNullKudf;
import io.confluent.ksql.function.udf.string.LCaseKudf;
import io.confluent.ksql.function.udf.string.LenKudf;
import io.confluent.ksql.function.udf.string.TrimKudf;
import io.confluent.ksql.function.udf.string.UCaseKudf;
import io.confluent.ksql.function.udf.structfieldextractor.FetchFieldFromStruct;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

@ThreadSafe
public class InternalFunctionRegistry implements MutableFunctionRegistry {

  private final Object lock = new Object();
  private final Map<String, UdfFactory> udfs = new ConcurrentHashMap<>();
  private final Map<String, AggregateFunctionFactory> udafs = new ConcurrentHashMap<>();
  private final FunctionNameValidator functionNameValidator = new FunctionNameValidator();

  public InternalFunctionRegistry() {
    new BuiltInInitializer(this).init();
  }

  public UdfFactory getUdfFactory(final String functionName) {
    final UdfFactory udfFactory = udfs.get(functionName.toUpperCase());
    if (udfFactory == null) {
      throw new KsqlException("Can't find any functions with the name '" + functionName + "'");
    }
    return udfFactory;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void addFunction(final KsqlFunction ksqlFunction) {
    final UdfFactory udfFactory = udfs.get(ksqlFunction.getFunctionName().toUpperCase());
    if (udfFactory == null) {
      throw new KsqlException("Unknown function factory: " + ksqlFunction.getFunctionName());
    }
    udfFactory.addFunction(ksqlFunction);
  }

  @Override
  public UdfFactory ensureFunctionFactory(final UdfFactory factory) {
    validateFunctionName(factory.getName());

    synchronized (lock) {
      final String functionName = factory.getName().toUpperCase();
      if (udafs.containsKey(functionName)) {
        throw new KsqlException("UdfFactory already registered as aggregate: " + functionName);
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
  }

  @Override
  public boolean isAggregate(final String functionName) {
    return udafs.containsKey(functionName.toUpperCase());
  }

  @Override
  public KsqlAggregateFunction getAggregate(
      final String functionName,
      final Schema argumentType
  ) {
    final AggregateFunctionFactory udafFactory = udafs.get(functionName.toUpperCase());
    if (udafFactory == null) {
      throw new KsqlException("No aggregate function with name " + functionName + " exists!");
    }

    return udafFactory.getProperAggregateFunction(Collections.singletonList(argumentType));
  }

  @Override
  public void addAggregateFunctionFactory(final AggregateFunctionFactory aggregateFunctionFactory) {
    final String functionName = aggregateFunctionFactory.getName().toUpperCase();
    validateFunctionName(functionName);

    synchronized (lock) {
      if (udfs.containsKey(functionName)) {
        throw new KsqlException(
            "Aggregate function already registered as non-aggregate: " + functionName);
      }

      if (udafs.putIfAbsent(functionName, aggregateFunctionFactory) != null) {
        throw new KsqlException("Aggregate function already registered: " + functionName);
      }
    }
  }

  @Override
  public List<UdfFactory> listFunctions() {
    return new ArrayList<>(udfs.values());
  }

  @Override
  public AggregateFunctionFactory getAggregateFactory(final String functionName) {
    final AggregateFunctionFactory udafFactory = udafs.get(functionName.toUpperCase());
    if (udafFactory == null) {
      throw new KsqlException(
          "Can not find any aggregate functions with the name '" + functionName + "'");
    }

    return udafFactory;
  }

  @Override
  public List<AggregateFunctionFactory> listAggregateFunctions() {
    return new ArrayList<>(udafs.values());
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

    private void init() {
      addStringFunctions();
      addMathFunctions();
      addGeoFunctions();
      addJsonFunctions();
      addStructFieldFetcher();
      addUdafFunctions();
    }

    private void addStringFunctions() {

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
          "LCASE", LCaseKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
          "UCASE", UCaseKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
          ConcatKudf.NAME, ConcatKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
          "TRIM", TrimKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA,
              Schema.OPTIONAL_STRING_SCHEMA),
          "IFNULL", IfNullKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_INT32_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
          "LEN",
          LenKudf.class));
    }

    private void addMathFunctions() {

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_FLOAT64_SCHEMA,
          ImmutableList.of(Schema.OPTIONAL_FLOAT64_SCHEMA),
          AbsKudf.NAME,
          AbsKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_FLOAT64_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA),
          AbsKudf.NAME,
          AbsKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_FLOAT64_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
          "CEIL",
          CeilKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_FLOAT64_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
          "FLOOR",
          FloorKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_INT64_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
          "ROUND",
          RoundKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_FLOAT64_SCHEMA,
          Collections.emptyList(),
          "RANDOM",
          RandomKudf.class));
    }

    private void addGeoFunctions() {

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_FLOAT64_SCHEMA,
          ImmutableList.of(
              Schema.OPTIONAL_FLOAT64_SCHEMA,
              Schema.OPTIONAL_FLOAT64_SCHEMA,
              Schema.OPTIONAL_FLOAT64_SCHEMA,
              Schema.OPTIONAL_FLOAT64_SCHEMA,
              Schema.OPTIONAL_STRING_SCHEMA),
          "GEO_DISTANCE",
          GeoDistanceKudf.class));
    }

    private void addJsonFunctions() {

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
          JsonExtractStringKudf.NAME,
          JsonExtractStringKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_BOOLEAN_SCHEMA,
          ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
          "ARRAYCONTAINS",
          ArrayContainsKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_BOOLEAN_SCHEMA,
          ImmutableList.of(
              SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
              Schema.OPTIONAL_STRING_SCHEMA),
          "ARRAYCONTAINS",
          ArrayContainsKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_BOOLEAN_SCHEMA,
          ImmutableList.of(
              SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(),
              Schema.OPTIONAL_INT32_SCHEMA),
          "ARRAYCONTAINS",
          ArrayContainsKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_BOOLEAN_SCHEMA,
          ImmutableList.of(
              SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(),
              Schema.OPTIONAL_INT64_SCHEMA),
          "ARRAYCONTAINS",
          ArrayContainsKudf.class));

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_BOOLEAN_SCHEMA,
          ImmutableList.of(
              SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build(),
              Schema.OPTIONAL_FLOAT64_SCHEMA),
          "ARRAYCONTAINS",
          ArrayContainsKudf.class));
    }

    private void addStructFieldFetcher() {

      addBuiltInFunction(KsqlFunction.createLegacyBuiltIn(
          SchemaBuilder.struct().optional().build(),
          ImmutableList.of(
              SchemaBuilder.struct().optional().build(),
              Schema.STRING_SCHEMA),
          FetchFieldFromStruct.FUNCTION_NAME,
          FetchFieldFromStruct.class),
          true);
    }

    private void addUdafFunctions() {

      functionRegistry.addAggregateFunctionFactory(new CountAggFunctionFactory());
      functionRegistry.addAggregateFunctionFactory(new SumAggFunctionFactory());

      functionRegistry.addAggregateFunctionFactory(new MaxAggFunctionFactory());
      functionRegistry.addAggregateFunctionFactory(new MinAggFunctionFactory());

      functionRegistry.addAggregateFunctionFactory(new TopKAggregateFunctionFactory());
      functionRegistry.addAggregateFunctionFactory(new TopkDistinctAggFunctionFactory());
    }

    private void addBuiltInFunction(final KsqlFunction ksqlFunction) {
      addBuiltInFunction(ksqlFunction, false);
    }

    private void addBuiltInFunction(final KsqlFunction ksqlFunction, final boolean internal) {
      functionRegistry
          .ensureFunctionFactory(builtInUdfFactory(ksqlFunction, internal))
          .addFunction(ksqlFunction);
    }

    private static UdfFactory builtInUdfFactory(
        final KsqlFunction ksqlFunction,
        final boolean internal
    ) {
      final UdfMetadata metadata = new UdfMetadata(
          ksqlFunction.getFunctionName(),
          ksqlFunction.getDescription(),
          "Confluent",
          "",
          KsqlFunction.INTERNAL_PATH,
          internal);

      return new UdfFactory(ksqlFunction.getKudfClass(), metadata);
    }
  }
}
