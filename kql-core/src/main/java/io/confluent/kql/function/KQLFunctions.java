/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.function;

import io.confluent.kql.function.udaf.count.CountAggFunctionDeterminer;
import org.apache.kafka.connect.data.Schema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kql.function.udaf.sum.SumAggFunctionDeterminer;
import io.confluent.kql.function.udf.math.AbsKUDF;
import io.confluent.kql.function.udf.math.CeilKUDF;
import io.confluent.kql.function.udf.math.RandomKUDF;
import io.confluent.kql.function.udf.string.ConcatKUDF;
import io.confluent.kql.function.udf.math.FloorKUDF;
import io.confluent.kql.function.udf.string.IfNullKUDF;
import io.confluent.kql.function.udf.string.LCaseKUDF;
import io.confluent.kql.function.udf.string.LenKUDF;
import io.confluent.kql.function.udf.math.RoundKUDF;
import io.confluent.kql.function.udf.string.SubstringKUDF;
import io.confluent.kql.function.udf.string.TrimKUDF;
import io.confluent.kql.function.udf.string.UCaseKUDF;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.util.ExpressionTypeManager;
import io.confluent.kql.util.KQLException;

public class KQLFunctions {

  public static Map<String, KQLFunction> kqlFunctionMap = new HashMap<>();
  public static Map<String, KQLAggFunctionDeterminer> kqlAggregateFunctionMap = new HashMap<>();

  static {

    /***************************************
     * String functions                     *
     ****************************************/

    KQLFunction lcase = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING),
                                        "LCASE", LCaseKUDF.class);
    KQLFunction ucase = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING),
                                        "UCASE", UCaseKUDF.class);
    KQLFunction substring = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type
                                                                                  .STRING,
                                                                              Schema.Type
                                                                                  .INT32,
                                                                              Schema.Type
                                                                                  .INT32),
                                            "SUBSTRING", SubstringKUDF
                                                .class);
    KQLFunction concat = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING,
                                                                           Schema.Type.STRING),
                                         "CONCAT", ConcatKUDF.class);

    KQLFunction trim = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING),
                                       "TRIM", TrimKUDF.class);

    KQLFunction ifNull = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING,
                                                                           Schema.Type.STRING),
                                         "IFNULL", IfNullKUDF.class);
    KQLFunction len = new KQLFunction(Schema.Type.INT32, Arrays.asList(Schema.Type.STRING),
                                      "LEN", LenKUDF.class);

    /***************************************
     * Math functions                      *
     ***************************************/

    KQLFunction abs = new KQLFunction(Schema.Type.FLOAT64, Arrays.asList(Schema.Type.FLOAT64),
                                      "ABS", AbsKUDF.class);
    KQLFunction ceil = new KQLFunction(Schema.Type.FLOAT64, Arrays.asList(Schema.Type.FLOAT64),
                                       "CEIL", CeilKUDF.class);
    KQLFunction floor = new KQLFunction(Schema.Type.FLOAT64, Arrays.asList(Schema.Type.FLOAT64),
                                        "FLOOR", FloorKUDF.class);
    KQLFunction
        round =
        new KQLFunction(Schema.Type.INT64, Arrays.asList(Schema.Type.FLOAT64), "ROUND",
                        RoundKUDF.class);
    KQLFunction random = new KQLFunction(Schema.Type.FLOAT64, new ArrayList<>(), "RANDOM",
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
    return kqlFunctionMap.get(functionName.toUpperCase());
  }

  public static void addFunction(KQLFunction kqlFunction) {
    kqlFunctionMap.put(kqlFunction.getFunctionName().toUpperCase(), kqlFunction);
  }

  public static boolean isAnAggregateFunction(String functionName) {
    if (kqlAggregateFunctionMap.get(functionName.toUpperCase()) != null) {
      return true;
    }
    return false;
  }

  public static KQLAggregateFunction getAggregateFunction(String functionName, List<Expression>
      functionArgs, Schema schema) {
    KQLAggFunctionDeterminer kqlAggFunctionDeterminer = kqlAggregateFunctionMap.get(functionName.toUpperCase());
    if (kqlAggFunctionDeterminer == null) {
      throw new KQLException("No aggregate function with name " + functionName + " exists!");
    }
    ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
    Schema.Type expressionType = expressionTypeManager.getExpressionType(functionArgs.get(0));
    KQLAggregateFunction aggregateFunction = kqlAggFunctionDeterminer.getProperAggregateFunction(Arrays.asList(expressionType));
    return aggregateFunction;
  }

  public static void addAggregateFunctionDeterminer(KQLAggFunctionDeterminer kqlAggFunctionDeterminer) {
    kqlAggregateFunctionMap.put(kqlAggFunctionDeterminer.functionName, kqlAggFunctionDeterminer);
  }


}
