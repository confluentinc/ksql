/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.function;

import io.confluent.kql.function.udaf.count.CountAggFunctionDeterminer;
import io.confluent.kql.function.udaf.sum.SumAggFunctionDeterminer;
import io.confluent.kql.function.udf.math.AbsKUDF;
import io.confluent.kql.function.udf.math.CeilKUDF;
import io.confluent.kql.function.udf.math.FloorKUDF;
import io.confluent.kql.function.udf.math.RandomKUDF;
import io.confluent.kql.function.udf.math.RoundKUDF;
import io.confluent.kql.function.udf.string.ConcatKUDF;
import io.confluent.kql.function.udf.string.IfNullKUDF;
import io.confluent.kql.function.udf.string.LCaseKUDF;
import io.confluent.kql.function.udf.string.LenKUDF;
import io.confluent.kql.function.udf.string.SubstringKUDF;
import io.confluent.kql.function.udf.string.TrimKUDF;
import io.confluent.kql.function.udf.string.UCaseKUDF;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.util.ExpressionTypeManager;
import io.confluent.kql.util.KQLException;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KQLFunctions {

  public static Map<String, KQLFunction> kqlFunctionMap = new HashMap<>();
  public static Map<String, KQLAggFunctionDeterminer> kqlAggregateFunctionMap = new HashMap<>();

  static {

    /***************************************
     * String functions                     *
     ****************************************/

    KQLFunction lcase = new KQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                        "LCASE", LCaseKUDF.class);
    KQLFunction ucase = new KQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                        "UCASE", UCaseKUDF.class);
    KQLFunction substring = new KQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema
                                                                                  .STRING_SCHEMA,
                                                                              Schema
                                                                                  .INT32_SCHEMA,
                                                                              Schema
                                                                                  .INT32_SCHEMA),
                                            "SUBSTRING", SubstringKUDF
                                                .class);
    KQLFunction concat = new KQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA,
                                                                           Schema.STRING_SCHEMA),
                                         "CONCAT", ConcatKUDF.class);

    KQLFunction trim = new KQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                       "TRIM", TrimKUDF.class);

    KQLFunction ifNull = new KQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA,
                                                                           Schema.STRING_SCHEMA),
                                         "IFNULL", IfNullKUDF.class);
    KQLFunction len = new KQLFunction(Schema.INT32_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                      "LEN", LenKUDF.class);

    /***************************************
     * Math functions                      *
     ***************************************/

    KQLFunction abs = new KQLFunction(Schema.FLOAT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
                                      "ABS", AbsKUDF.class);
    KQLFunction ceil = new KQLFunction(Schema.FLOAT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
                                       "CEIL", CeilKUDF.class);
    KQLFunction floor = new KQLFunction(Schema.FLOAT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
                                        "FLOOR", FloorKUDF.class);
    KQLFunction
        round =
        new KQLFunction(Schema.INT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA), "ROUND",
                        RoundKUDF.class);
    KQLFunction random = new KQLFunction(Schema.FLOAT64_SCHEMA, new ArrayList<>(), "RANDOM",
                                         RandomKUDF.class);



    addFunction(lcase);
    addFunction(ucase);
    addFunction(substring);
    addFunction(concat);
    addFunction(len);
    addFunction(trim);
    addFunction(ifNull);

    addFunction(abs);
    addFunction(ceil);
    addFunction(floor);
    addFunction(round);
    addFunction(random);

    /***************************************
     * UDAFs                               *
     ***************************************/

    addAggregateFunctionDeterminer(new CountAggFunctionDeterminer());
    addAggregateFunctionDeterminer(new SumAggFunctionDeterminer());

  }

  public static KQLFunction getFunction(String functionName) {
    return kqlFunctionMap.get(functionName);
  }

  public static void addFunction(KQLFunction kqlFunction) {
    kqlFunctionMap.put(kqlFunction.getFunctionName().toUpperCase(), kqlFunction);
  }

  public static boolean isAnAggregateFunction(String functionName) {
    if (kqlAggregateFunctionMap.get(functionName) != null) {
      return true;
    }
    return false;
  }

  public static KQLAggregateFunction getAggregateFunction(String functionName, List<Expression>
      functionArgs, Schema schema) {
    KQLAggFunctionDeterminer kqlAggFunctionDeterminer = kqlAggregateFunctionMap.get(functionName);
    if (kqlAggFunctionDeterminer == null) {
      throw new KQLException("No aggregate function with name " + functionName + " exists!");
    }
    ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
    Schema expressionType = expressionTypeManager.getExpressionType(functionArgs.get(0));
    KQLAggregateFunction aggregateFunction = kqlAggFunctionDeterminer.getProperAggregateFunction(Arrays.asList(expressionType));
    return aggregateFunction;
  }

  public static void addAggregateFunctionDeterminer(KQLAggFunctionDeterminer kqlAggFunctionDeterminer) {
    kqlAggregateFunctionMap.put(kqlAggFunctionDeterminer.functionName, kqlAggFunctionDeterminer);
  }


}
