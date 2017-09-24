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

import io.confluent.ksql.function.udaf.count.CountAggFunctionDeterminer;
import io.confluent.ksql.function.udaf.max.MaxAggFunctionDeterminer;
import io.confluent.ksql.function.udaf.min.MinAggFunctionDeterminer;
import io.confluent.ksql.function.udaf.sum.SumAggFunctionDeterminer;
import io.confluent.ksql.function.udf.datetime.StringToTimestamp;
import io.confluent.ksql.function.udf.datetime.TimestampToString;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KsqlFunctions {

  public static Map<String, KsqlFunction> ksqlFunctionMap = new HashMap<>();
  public static Map<String, KsqlAggFunctionDeterminer> ksqlAggregateFunctionMap = new HashMap<>();

  static {

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


    /***************************************
     * UDAFs                               *
     ***************************************/

    addAggregateFunctionDeterminer(new CountAggFunctionDeterminer());
    addAggregateFunctionDeterminer(new SumAggFunctionDeterminer());

    addAggregateFunctionDeterminer(new MaxAggFunctionDeterminer());
    addAggregateFunctionDeterminer(new MinAggFunctionDeterminer());

  }

  public static KsqlFunction getFunction(String functionName) {
    return ksqlFunctionMap.get(functionName);
  }

  public static void addFunction(KsqlFunction ksqlFunction) {
    ksqlFunctionMap.put(ksqlFunction.getFunctionName().toUpperCase(), ksqlFunction);
  }

  public static boolean isAnAggregateFunction(String functionName) {
    return ksqlAggregateFunctionMap.get(functionName) != null;
  }

  public static KsqlAggregateFunction getAggregateFunction(String functionName, List<Expression>
      functionArgs, Schema schema) {
    KsqlAggFunctionDeterminer ksqlAggFunctionDeterminer = ksqlAggregateFunctionMap
        .get(functionName);
    if (ksqlAggFunctionDeterminer == null) {
      throw new KsqlException("No aggregate function with name " + functionName + " exists!");
    }
    ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
    Schema expressionType = expressionTypeManager.getExpressionType(functionArgs.get(0));
    KsqlAggregateFunction aggregateFunction =
        ksqlAggFunctionDeterminer.getProperAggregateFunction(Arrays.asList(expressionType));
    return aggregateFunction;
  }

  public static void addAggregateFunctionDeterminer(KsqlAggFunctionDeterminer
                                                        ksqlAggFunctionDeterminer) {
    ksqlAggregateFunctionMap.put(ksqlAggFunctionDeterminer.functionName, ksqlAggFunctionDeterminer);
  }


}
