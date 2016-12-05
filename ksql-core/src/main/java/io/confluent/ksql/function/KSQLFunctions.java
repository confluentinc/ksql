package io.confluent.ksql.function;


import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.function.udf.math.Abs_KUDF;
import io.confluent.ksql.function.udf.math.Ceil_KUDF;
import io.confluent.ksql.function.udf.math.Random_KUDF;
import io.confluent.ksql.function.udf.string.Concat_KUDF;
import io.confluent.ksql.function.udf.math.Floor_KUDF;
import io.confluent.ksql.function.udf.string.IfNull_KUDF;
import io.confluent.ksql.function.udf.string.LCase_KUDF;
import io.confluent.ksql.function.udf.string.Len_KUDF;
import io.confluent.ksql.function.udf.math.Round_KUDF;
import io.confluent.ksql.function.udf.string.Substring_KUDF;
import io.confluent.ksql.function.udf.string.Trim_KUDF;
import io.confluent.ksql.function.udf.string.UCase_KUDF;

public class KSQLFunctions {

  public static Map<String, KSQLFunction> ksqlFunctionMap = new HashMap<>();

  static {

    /***************************************
    * String functions                     *
    ****************************************/

    KSQLFunction lcase = new KSQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING),
                                          "LCASE", LCase_KUDF.class);
    KSQLFunction ucase = new KSQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING),
                                          "UCASE", UCase_KUDF.class);
    KSQLFunction substring = new KSQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type
                                                                                    .STRING,
                                                                                Schema.Type
                                                                                    .INT32,
                                                                                Schema.Type
                                                                                    .INT32),
                                              "SUBSTRING", Substring_KUDF
                                                  .class);
    KSQLFunction concat = new KSQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING,
                                                                             Schema.Type.STRING),
                                           "CONCAT", Concat_KUDF.class);

    KSQLFunction trim = new KSQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING),
                                         "TRIM", Trim_KUDF.class);

    KSQLFunction ifNull = new KSQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING,
                                                                             Schema.Type.STRING),
                                           "IFNULL", IfNull_KUDF.class);
    KSQLFunction len = new KSQLFunction(Schema.Type.INT32, Arrays.asList(Schema.Type.STRING),
                                        "LEN", Len_KUDF.class);

    /***************************************
     * Math functions                      *
     ***************************************/

    KSQLFunction abs = new KSQLFunction(Schema.Type.FLOAT64, Arrays.asList(Schema.Type.FLOAT64),
                                        "ABS", Abs_KUDF.class);
    KSQLFunction ceil = new KSQLFunction(Schema.Type.INT64, Arrays.asList(Schema.Type.FLOAT64),
                                         "CEIL", Ceil_KUDF.class);
    KSQLFunction floor = new KSQLFunction(Schema.Type.INT64, Arrays.asList(Schema.Type.FLOAT64),
                                          "FLOOR", Floor_KUDF.class);
    KSQLFunction round = new KSQLFunction(Schema.Type.FLOAT64, Arrays.asList(Schema.Type.FLOAT64)
                                      , "ROUND", Round_KUDF.class);
    KSQLFunction random = new KSQLFunction(Schema.Type.FLOAT64, new ArrayList<>(), "RANDOM",
                                           Random_KUDF.class);



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

  }

  public static KSQLFunction getFunction(String functionName) {
    return ksqlFunctionMap.get(functionName.toUpperCase());
  }

  public static void addFunction(KSQLFunction ksqlFunction) {
    ksqlFunctionMap.put(ksqlFunction.getFunctionName().toUpperCase(), ksqlFunction);
  }

}
