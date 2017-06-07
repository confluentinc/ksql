/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function;

import io.confluent.ksql.function.udaf.count.CountAggFunctionDeterminer;
import io.confluent.ksql.function.udaf.sum.SumAggFunctionDeterminer;
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

public class KSQLFunctions {

  public static Map<String, KsqlFunction> ksqlFunctionMap = new HashMap<>();
  public static Map<String, KsqlAggFunctionDeterminer> ksqlAggregateFunctionMap = new HashMap<>();

  static {

    /***************************************
     * String functions                     *
     ****************************************/

    KsqlFunction lcase = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                        "LCASE", LCaseKudf.class);
    KsqlFunction ucase = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                        "UCASE", UCaseKudf.class);
    KsqlFunction substring = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema
                                                                                  .STRING_SCHEMA,
                                                                              Schema
                                                                                  .INT32_SCHEMA,
                                                                              Schema
                                                                                  .INT32_SCHEMA),
                                            "SUBSTRING", SubstringKudf
                                                .class);
    KsqlFunction concat = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA,
                                                                           Schema.STRING_SCHEMA),
                                         "CONCAT", ConcatKudf.class);

    KsqlFunction trim = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                       "TRIM", TrimKudf.class);

    KsqlFunction ifNull = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA,
                                                                           Schema.STRING_SCHEMA),
                                         "IFNULL", IfNullKudf.class);
    KsqlFunction len = new KsqlFunction(Schema.INT32_SCHEMA, Arrays.asList(Schema.STRING_SCHEMA),
                                      "LEN", LenKudf.class);

    /***************************************
     * Math functions                      *
     ***************************************/

    KsqlFunction abs = new KsqlFunction(Schema.FLOAT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
                                      "ABS", AbsKudf.class);
    KsqlFunction ceil = new KsqlFunction(Schema.FLOAT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
                                       "CEIL", CeilKudf.class);
    KsqlFunction floor = new KsqlFunction(Schema.FLOAT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA),
                                        "FLOOR", FloorKudf.class);
    KsqlFunction
        round =
        new KsqlFunction(Schema.INT64_SCHEMA, Arrays.asList(Schema.FLOAT64_SCHEMA), "ROUND",
                        RoundKudf.class);
    KsqlFunction random = new KsqlFunction(Schema.FLOAT64_SCHEMA, new ArrayList<>(), "RANDOM",
                                         RandomKudf.class);



    /***************************************
     * JSON functions                     *
     ****************************************/

    KsqlFunction getStringFromJson = new KsqlFunction(Schema.STRING_SCHEMA, Arrays.asList(Schema
                                                                                              .STRING_SCHEMA, Schema.STRING_SCHEMA),
                                          "GETSTREAMFROMJSON", JsonExtractStringKudf.class);

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

    addFunction(getStringFromJson);

    /***************************************
     * UDAFs                               *
     ***************************************/

    addAggregateFunctionDeterminer(new CountAggFunctionDeterminer());
    addAggregateFunctionDeterminer(new SumAggFunctionDeterminer());

  }

  public static KsqlFunction getFunction(String functionName) {
    return ksqlFunctionMap.get(functionName);
  }

  public static void addFunction(KsqlFunction ksqlFunction) {
    ksqlFunctionMap.put(ksqlFunction.getFunctionName().toUpperCase(), ksqlFunction);
  }

  public static boolean isAnAggregateFunction(String functionName) {
    if (ksqlAggregateFunctionMap.get(functionName) != null) {
      return true;
    }
    return false;
  }

  public static KsqlAggregateFunction getAggregateFunction(String functionName, List<Expression>
      functionArgs, Schema schema) {
    KsqlAggFunctionDeterminer ksqlAggFunctionDeterminer = ksqlAggregateFunctionMap.get(functionName);
    if (ksqlAggFunctionDeterminer == null) {
      throw new KsqlException("No aggregate function with name " + functionName + " exists!");
    }
    ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
    Schema expressionType = expressionTypeManager.getExpressionType(functionArgs.get(0));
    KsqlAggregateFunction aggregateFunction = ksqlAggFunctionDeterminer.getProperAggregateFunction(Arrays.asList(expressionType));
    return aggregateFunction;
  }

  public static void addAggregateFunctionDeterminer(KsqlAggFunctionDeterminer ksqlAggFunctionDeterminer) {
    ksqlAggregateFunctionMap.put(ksqlAggFunctionDeterminer.functionName, ksqlAggFunctionDeterminer);
  }


}
