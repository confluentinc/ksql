/**
 * Copyright 2017 Confluent Inc.
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


import io.confluent.ksql.function.udaf.count.CountAggFunctionFactory;
import io.confluent.ksql.function.udaf.max.MaxAggFunctionFactory;
import io.confluent.ksql.function.udaf.min.MinAggFunctionFactory;
import io.confluent.ksql.function.udaf.sum.SumAggFunctionFactory;
import io.confluent.ksql.function.udaf.topk.TopKAggregateFunctionFactory;
import io.confluent.ksql.function.udaf.topkdistinct.TopkDistinctAggFunctionFactory;
import io.confluent.ksql.function.udf.datetime.StringToTimestamp;
import io.confluent.ksql.function.udf.datetime.TimestampToString;
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
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InternalFunctionRegistry implements FunctionRegistry {

  private final Map<String, UdfFactory> ksqlFunctionMap ;
  private final Map<String, AggregateFunctionFactory> aggregateFunctionMap;
  private final FunctionNameValidator functionNameValidator = new FunctionNameValidator();

  public InternalFunctionRegistry() {
    this(new HashMap<>(), new HashMap<>());
    init();
  }

  private InternalFunctionRegistry(
      final Map<String, UdfFactory> ksqlFunctionMap,
      final Map<String, AggregateFunctionFactory> aggregateFunctionMap
  ) {
    this.ksqlFunctionMap = ksqlFunctionMap;
    this.aggregateFunctionMap = aggregateFunctionMap;
  }

  private void init() {
    addStringFunctions();
    addMathFunctions();
    addDateTimeFunctions();
    addGeoFunctions();
    addJsonFunctions();
    addUdafFunctions();
  }

  public UdfFactory getUdfFactory(final String functionName) {
    return ksqlFunctionMap.get(functionName.toUpperCase());
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean addFunction(final KsqlFunction ksqlFunction) {
    if (!functionNameValidator.test(ksqlFunction.getFunctionName())) {
      throw new KsqlException(ksqlFunction.getFunctionName() + " is not a valid function name."
          + " Function names must be valid java identifiers and not a KSQL reserved word"
      );
    }

    final String key = ksqlFunction.getFunctionName().toUpperCase();
    ksqlFunctionMap.compute(key, (s, udf) -> {
      if (udf == null) {
        udf = new UdfFactory(key, ksqlFunction.getKudfClass());
      }
      udf.addFunction(ksqlFunction);
      return udf;
    });

    return true;
  }

  @Override
  public boolean isAggregate(final String functionName) {
    return aggregateFunctionMap.containsKey(functionName.toUpperCase());
  }

  @Override
  public KsqlAggregateFunction getAggregate(
      final String functionName,
      final Schema argumentType
  ) {
    AggregateFunctionFactory aggregateFunctionFactory
        = aggregateFunctionMap.get(functionName.toUpperCase());
    if (aggregateFunctionFactory == null) {
      throw new KsqlException("No aggregate function with name " + functionName + " exists!");
    }
    return aggregateFunctionFactory.getProperAggregateFunction(
        Collections.singletonList(argumentType));
  }

  @Override
  public void addAggregateFunctionFactory(final AggregateFunctionFactory aggregateFunctionFactory) {
    aggregateFunctionMap.put(
        aggregateFunctionFactory.functionName.toUpperCase(),
        aggregateFunctionFactory);
  }

  @Override
  public FunctionRegistry copy() {
    return new InternalFunctionRegistry(
        new HashMap<>(ksqlFunctionMap),
        new HashMap<>(aggregateFunctionMap));
  }

  private void addStringFunctions() {

    /***************************************
     * String functions                     *
     ****************************************/

    KsqlFunction lcase = new KsqlFunction(Schema.OPTIONAL_STRING_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA),
        "LCASE", LCaseKudf.class);
    addFunction(lcase);

    KsqlFunction ucase = new KsqlFunction(Schema.OPTIONAL_STRING_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA),
        "UCASE", UCaseKudf.class);
    addFunction(ucase);


    KsqlFunction concat = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA,
                                                                           Schema.STRING_SCHEMA),
                                         "CONCAT", ConcatKudf.class);

    addFunction(concat);

    KsqlFunction trim = new KsqlFunction(Schema.OPTIONAL_STRING_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA),
        "TRIM", TrimKudf.class);
    addFunction(trim);

    KsqlFunction ifNull = new KsqlFunction(Schema.OPTIONAL_STRING_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA,
            Schema.OPTIONAL_STRING_SCHEMA),
        "IFNULL", IfNullKudf.class);
    addFunction(ifNull);

    KsqlFunction len = new KsqlFunction(
        Schema.OPTIONAL_INT32_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA),
        "LEN",
        LenKudf.class);
    addFunction(len);

  }

  private void addMathFunctions() {
    KsqlFunction abs = new KsqlFunction(Schema.OPTIONAL_FLOAT64_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_FLOAT64_SCHEMA),
        "ABS", AbsKudf.class);
    addFunction(abs);
    addFunction(new KsqlFunction(Schema.OPTIONAL_FLOAT64_SCHEMA,
        Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA),
        "ABS",
        AbsKudf.class));

    KsqlFunction ceil = new KsqlFunction(Schema.OPTIONAL_FLOAT64_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_FLOAT64_SCHEMA),
        "CEIL", CeilKudf.class);
    addFunction(ceil);

    KsqlFunction floor = new KsqlFunction(Schema.OPTIONAL_FLOAT64_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_FLOAT64_SCHEMA),
        "FLOOR", FloorKudf.class);
    addFunction(floor);

    KsqlFunction
        round =
        new KsqlFunction(Schema.OPTIONAL_INT64_SCHEMA,
            Arrays.asList(Schema.OPTIONAL_FLOAT64_SCHEMA),
            "ROUND", RoundKudf.class);
    addFunction(round);

    KsqlFunction random = new KsqlFunction(Schema.OPTIONAL_FLOAT64_SCHEMA, new ArrayList<>(),
        "RANDOM", RandomKudf.class);
    addFunction(random);


  }


  private void addDateTimeFunctions() {

    KsqlFunction timestampToString = new KsqlFunction(
        Schema.OPTIONAL_STRING_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
        "TIMESTAMPTOSTRING",
        TimestampToString.class);
    addFunction(timestampToString);

    KsqlFunction stringToTimestamp = new KsqlFunction(
        Schema.OPTIONAL_INT64_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
        "STRINGTOTIMESTAMP",
        StringToTimestamp.class);
    addFunction(stringToTimestamp);

  }

  private void addGeoFunctions() {
    KsqlFunction geoDistance = new KsqlFunction(
        Schema.OPTIONAL_FLOAT64_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_FLOAT64_SCHEMA,
            Schema.OPTIONAL_FLOAT64_SCHEMA,
            Schema.OPTIONAL_FLOAT64_SCHEMA,
            Schema.OPTIONAL_FLOAT64_SCHEMA,
            Schema.OPTIONAL_STRING_SCHEMA),
        "GEO_DISTANCE", GeoDistanceKudf.class);
    addFunction(geoDistance);

  }

  private void addJsonFunctions() {

    KsqlFunction getStringFromJson = new KsqlFunction(
        Schema.OPTIONAL_STRING_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
        "EXTRACTJSONFIELD", JsonExtractStringKudf.class);
    addFunction(getStringFromJson);

    KsqlFunction jsonArrayContainsString = new KsqlFunction(
        Schema.OPTIONAL_BOOLEAN_SCHEMA,
        Arrays.asList(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
        "ARRAYCONTAINS", ArrayContainsKudf.class);
    addFunction(jsonArrayContainsString);

    addFunction(new KsqlFunction(
        Schema.OPTIONAL_BOOLEAN_SCHEMA,
        Arrays.asList(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
            Schema.OPTIONAL_STRING_SCHEMA),
        "ARRAYCONTAINS", ArrayContainsKudf.class));

    addFunction(new KsqlFunction(
        Schema.OPTIONAL_BOOLEAN_SCHEMA,
        Arrays.asList(SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(),
            Schema.OPTIONAL_INT32_SCHEMA),
        "ARRAYCONTAINS", ArrayContainsKudf.class));

    addFunction(new KsqlFunction(
        Schema.OPTIONAL_BOOLEAN_SCHEMA,
        Arrays.asList(SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(),
            Schema.OPTIONAL_INT64_SCHEMA),
        "ARRAYCONTAINS", ArrayContainsKudf.class));

    addFunction(new KsqlFunction(
        Schema.OPTIONAL_BOOLEAN_SCHEMA,
        Arrays.asList(SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build(),
            Schema.OPTIONAL_FLOAT64_SCHEMA),
        "ARRAYCONTAINS", ArrayContainsKudf.class));
  }

  private void addUdafFunctions() {
    addAggregateFunctionFactory(new CountAggFunctionFactory());
    addAggregateFunctionFactory(new SumAggFunctionFactory());

    addAggregateFunctionFactory(new MaxAggFunctionFactory());
    addAggregateFunctionFactory(new MinAggFunctionFactory());

    addAggregateFunctionFactory(new TopKAggregateFunctionFactory());
    addAggregateFunctionFactory(new TopkDistinctAggFunctionFactory());
  }

}
