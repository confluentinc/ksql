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
import io.confluent.ksql.function.udf.string.SubstringKudf;
import io.confluent.ksql.function.udf.string.TrimKudf;
import io.confluent.ksql.function.udf.string.UCaseKudf;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.ExpressionTypeManager;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FunctionRegistry {

  private Map<String, KsqlFunction> ksqlFunctionMap = new HashMap<>();
  private Map<String, AggregateFunctionFactory> aggregateFunctionMap = new HashMap<>();

  public FunctionRegistry() {
    init();
  }

  private void init() {

    /***************************************
     * String functions                     *
     ****************************************/

    KsqlFunction lcase = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                        "LCASE", LCaseKudf.class);
    addFunction(lcase);

    KsqlFunction ucase = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                        "UCASE", UCaseKudf.class);
    addFunction(ucase);

    KsqlFunction substring = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema
                                                                                  .STRING_SCHEMA,
                                                                              Schema
                                                                                  .INT32_SCHEMA,
                                                                              Schema
                                                                                  .INT32_SCHEMA),
                                            "SUBSTRING", SubstringKudf
                                                .class);
    addFunction(substring);

    KsqlFunction concat = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA,
                                                                           Schema.STRING_SCHEMA),
                                         "CONCAT", ConcatKudf.class);
    addFunction(concat);

    KsqlFunction trim = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                       "TRIM", TrimKudf.class);
    addFunction(trim);

    KsqlFunction ifNull = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA,
                                                                           Schema.STRING_SCHEMA),
                                         "IFNULL", IfNullKudf.class);
    addFunction(ifNull);

    KsqlFunction len = new KsqlFunction(Schema.INT32_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                      "LEN", LenKudf.class);
    addFunction(len);

    /***************************************
     * Math functions                      *
     ***************************************/

    KsqlFunction abs = new KsqlFunction(Schema.FLOAT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
                                      "ABS", AbsKudf.class);
    addFunction(abs);

    KsqlFunction ceil = new KsqlFunction(Schema.FLOAT64_SCHEMA,
                                         Arrays.asList(Schema.FLOAT64_SCHEMA),
                                       "CEIL", CeilKudf.class);
    addFunction(ceil);

    KsqlFunction floor = new KsqlFunction(Schema.FLOAT64_SCHEMA,
                                          Arrays.asList(Schema.FLOAT64_SCHEMA),
                                        "FLOOR", FloorKudf.class);
    addFunction(floor);

    KsqlFunction
        round =
        new KsqlFunction(Schema.INT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
                         "ROUND", RoundKudf.class);
    addFunction(round);

    KsqlFunction random = new KsqlFunction(Schema.FLOAT64_SCHEMA, new ArrayList<>(),
                                           "RANDOM", RandomKudf.class);
    addFunction(random);


    /***************************************
     * Date/Time functions                      *
     ***************************************/
    KsqlFunction timestampToString = new KsqlFunction(Schema.STRING_SCHEMA,
                                                      Arrays.asList(Schema.INT64_SCHEMA,
                                                                    Schema.STRING_SCHEMA),
                                        "TIMESTAMPTOSTRING", TimestampToString.class);
    addFunction(timestampToString);

    KsqlFunction stringToTimestamp = new KsqlFunction(Schema.INT64_SCHEMA,
                                                      Arrays.asList(Schema.STRING_SCHEMA,
                                                                    Schema.STRING_SCHEMA),
                                                      "STRINGTOTIMESTAMP",
                                                      StringToTimestamp.class);
    addFunction(stringToTimestamp);

    /***************************************
     * JSON functions                     *
     ****************************************/

    KsqlFunction getStringFromJson = new KsqlFunction(
        Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA),
        "EXTRACTJSONFIELD", JsonExtractStringKudf.class);
    addFunction(getStringFromJson);

    KsqlFunction jsonArrayContainsString = new KsqlFunction(
            Schema.BOOLEAN_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA),
            "ARRAYCONTAINS", ArrayContainsKudf.class);
    addFunction(jsonArrayContainsString);

    addFunction(new KsqlFunction(
        Schema.BOOLEAN_SCHEMA,
        Arrays.asList(SchemaBuilder.array(Schema.STRING_SCHEMA).build(), Schema.STRING_SCHEMA),
        "ARRAYCONTAINS", ArrayContainsKudf.class));

    addFunction(new KsqlFunction(
        Schema.BOOLEAN_SCHEMA,
        Arrays.asList(SchemaBuilder.array(Schema.INT32_SCHEMA).build(), Schema.INT32_SCHEMA),
        "ARRAYCONTAINS", ArrayContainsKudf.class));

    addFunction(new KsqlFunction(
        Schema.BOOLEAN_SCHEMA,
        Arrays.asList(SchemaBuilder.array(Schema.INT64_SCHEMA).build(), Schema.INT64_SCHEMA),
        "ARRAYCONTAINS", ArrayContainsKudf.class));

    addFunction(new KsqlFunction(
        Schema.BOOLEAN_SCHEMA,
        Arrays.asList(SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(), Schema.FLOAT64_SCHEMA),
        "ARRAYCONTAINS", ArrayContainsKudf.class));


    /***************************************
     * UDAFs                               *
     ***************************************/

    addAggregateFunctionFactory(new CountAggFunctionFactory());
    addAggregateFunctionFactory(new SumAggFunctionFactory());

    addAggregateFunctionFactory(new MaxAggFunctionFactory());
    addAggregateFunctionFactory(new MinAggFunctionFactory());

    addAggregateFunctionFactory(new TopKAggregateFunctionFactory());
    addAggregateFunctionFactory(new TopkDistinctAggFunctionFactory());

  }

  public KsqlFunction getFunction(String functionName) {
    return ksqlFunctionMap.get(functionName);
  }

  private void addFunction(KsqlFunction ksqlFunction) {
    ksqlFunctionMap.put(ksqlFunction.getFunctionName().toUpperCase(), ksqlFunction);
  }

  public boolean isAnAggregateFunction(String functionName) {
    return aggregateFunctionMap.containsKey(functionName);
  }

  public KsqlAggregateFunction getAggregateFunction(String functionName,
          List<Expression> functionArgs, Schema schema) {
    AggregateFunctionFactory aggregateFunctionFactory = aggregateFunctionMap.get(functionName);
    if (aggregateFunctionFactory == null) {
      throw new KsqlException("No aggregate function with name " + functionName + " exists!");
    }
    ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema, this);
    Schema expressionType = expressionTypeManager.getExpressionType(functionArgs.get(0));
    return aggregateFunctionFactory.getProperAggregateFunction(Arrays.asList(expressionType));
  }

  public void addAggregateFunctionFactory(AggregateFunctionFactory aggregateFunctionFactory) {
    aggregateFunctionMap.put(aggregateFunctionFactory.functionName, aggregateFunctionFactory);
  }


}
