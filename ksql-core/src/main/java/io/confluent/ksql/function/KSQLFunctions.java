/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function;

import io.confluent.ksql.function.udaf.count.CountAggFunctionDeterminer;
import io.confluent.ksql.function.udaf.sum.SumAggFunctionDeterminer;
import io.confluent.ksql.function.udf.math.AbsKUDF;
import io.confluent.ksql.function.udf.math.CeilKUDF;
import io.confluent.ksql.function.udf.math.FloorKUDF;
import io.confluent.ksql.function.udf.math.RandomKUDF;
import io.confluent.ksql.function.udf.math.RoundKUDF;
import io.confluent.ksql.function.udf.string.ConcatKUDF;
import io.confluent.ksql.function.udf.string.IfNullKUDF;
import io.confluent.ksql.function.udf.string.LCaseKUDF;
import io.confluent.ksql.function.udf.string.LenKUDF;
import io.confluent.ksql.function.udf.string.SubstringKUDF;
import io.confluent.ksql.function.udf.string.TrimKUDF;
import io.confluent.ksql.function.udf.string.UCaseKUDF;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.ExpressionTypeManager;
import io.confluent.ksql.util.KSQLException;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KSQLFunctions {

  public static Map<String, KSQLFunction> ksqlFunctionMap = new HashMap<>();
  public static Map<String, KSQLAggFunctionDeterminer> ksqlAggregateFunctionMap = new HashMap<>();

  static {

    /***************************************
     * String functions                     *
     ****************************************/

    KSQLFunction lcase = new KSQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                        "LCASE", LCaseKUDF.class);
    KSQLFunction ucase = new KSQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                        "UCASE", UCaseKUDF.class);
    KSQLFunction substring = new KSQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema
                                                                                  .STRING_SCHEMA,
                                                                              Schema
                                                                                  .INT32_SCHEMA,
                                                                              Schema
                                                                                  .INT32_SCHEMA),
                                            "SUBSTRING", SubstringKUDF
                                                .class);
    KSQLFunction concat = new KSQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA,
                                                                           Schema.STRING_SCHEMA),
                                         "CONCAT", ConcatKUDF.class);

    KSQLFunction trim = new KSQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                       "TRIM", TrimKUDF.class);

    KSQLFunction ifNull = new KSQLFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA,
                                                                           Schema.STRING_SCHEMA),
                                         "IFNULL", IfNullKUDF.class);
    KSQLFunction len = new KSQLFunction(Schema.INT32_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                      "LEN", LenKUDF.class);

    /***************************************
     * Math functions                      *
     ***************************************/

    KSQLFunction abs = new KSQLFunction(Schema.FLOAT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
                                      "ABS", AbsKUDF.class);
    KSQLFunction ceil = new KSQLFunction(Schema.FLOAT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
                                       "CEIL", CeilKUDF.class);
    KSQLFunction floor = new KSQLFunction(Schema.FLOAT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
                                        "FLOOR", FloorKUDF.class);
    KSQLFunction
        round =
        new KSQLFunction(Schema.INT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA), "ROUND",
                        RoundKUDF.class);
    KSQLFunction random = new KSQLFunction(Schema.FLOAT64_SCHEMA, new ArrayList<>(), "RANDOM",
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

  public static KSQLFunction getFunction(String functionName) {
    return ksqlFunctionMap.get(functionName);
  }

  public static void addFunction(KSQLFunction ksqlFunction) {
    ksqlFunctionMap.put(ksqlFunction.getFunctionName().toUpperCase(), ksqlFunction);
  }

  public static boolean isAnAggregateFunction(String functionName) {
    if (ksqlAggregateFunctionMap.get(functionName) != null) {
      return true;
    }
    return false;
  }

  public static KSQLAggregateFunction getAggregateFunction(String functionName, List<Expression>
      functionArgs, Schema schema) {
    KSQLAggFunctionDeterminer ksqlAggFunctionDeterminer = ksqlAggregateFunctionMap.get(functionName);
    if (ksqlAggFunctionDeterminer == null) {
      throw new KSQLException("No aggregate function with name " + functionName + " exists!");
    }
    ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
    Schema expressionType = expressionTypeManager.getExpressionType(functionArgs.get(0));
    KSQLAggregateFunction aggregateFunction = ksqlAggFunctionDeterminer.getProperAggregateFunction(Arrays.asList(expressionType));
    return aggregateFunction;
  }

  public static void addAggregateFunctionDeterminer(KSQLAggFunctionDeterminer ksqlAggFunctionDeterminer) {
    ksqlAggregateFunctionMap.put(ksqlAggFunctionDeterminer.functionName, ksqlAggFunctionDeterminer);
  }


}
