package io.confluent.kql.function;


import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kql.function.udf.math.Abs_KUDF;
import io.confluent.kql.function.udf.math.Ceil_KUDF;
import io.confluent.kql.function.udf.math.Random_KUDF;
import io.confluent.kql.function.udf.string.Concat_KUDF;
import io.confluent.kql.function.udf.math.Floor_KUDF;
import io.confluent.kql.function.udf.string.IfNull_KUDF;
import io.confluent.kql.function.udf.string.LCase_KUDF;
import io.confluent.kql.function.udf.string.Len_KUDF;
import io.confluent.kql.function.udf.math.Round_KUDF;
import io.confluent.kql.function.udf.string.Substring_KUDF;
import io.confluent.kql.function.udf.string.Trim_KUDF;
import io.confluent.kql.function.udf.string.UCase_KUDF;

public class KQLFunctions {

  public static Map<String, KQLFunction> kqlFunctionMap = new HashMap<>();

  static {

    /***************************************
    * String functions                     *
    ****************************************/

    KQLFunction lcase = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING),
                                          "LCASE", LCase_KUDF.class);
    KQLFunction ucase = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING),
                                          "UCASE", UCase_KUDF.class);
    KQLFunction substring = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type
                                                                                    .STRING,
                                                                                Schema.Type
                                                                                    .INT32,
                                                                                Schema.Type
                                                                                    .INT32),
                                              "SUBSTRING", Substring_KUDF
                                                  .class);
    KQLFunction concat = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING,
                                                                             Schema.Type.STRING),
                                           "CONCAT", Concat_KUDF.class);

    KQLFunction trim = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING),
                                         "TRIM", Trim_KUDF.class);

    KQLFunction ifNull = new KQLFunction(Schema.Type.STRING, Arrays.asList(Schema.Type.STRING,
                                                                             Schema.Type.STRING),
                                           "IFNULL", IfNull_KUDF.class);
    KQLFunction len = new KQLFunction(Schema.Type.INT32, Arrays.asList(Schema.Type.STRING),
                                        "LEN", Len_KUDF.class);

    /***************************************
     * Math functions                      *
     ***************************************/

    KQLFunction abs = new KQLFunction(Schema.Type.FLOAT64, Arrays.asList(Schema.Type.FLOAT64),
                                        "ABS", Abs_KUDF.class);
    KQLFunction ceil = new KQLFunction(Schema.Type.FLOAT64, Arrays.asList(Schema.Type.FLOAT64),
                                         "CEIL", Ceil_KUDF.class);
    KQLFunction floor = new KQLFunction(Schema.Type.FLOAT64, Arrays.asList(Schema.Type.FLOAT64),
                                          "FLOOR", Floor_KUDF.class);
    KQLFunction round = new KQLFunction(Schema.Type.INT64, Arrays.asList(Schema.Type.FLOAT64)
                                      , "ROUND", Round_KUDF.class);
    KQLFunction random = new KQLFunction(Schema.Type.FLOAT64, new ArrayList<>(), "RANDOM",
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

  public static KQLFunction getFunction(String functionName) {
    return kqlFunctionMap.get(functionName.toUpperCase());
  }

  public static void addFunction(KQLFunction kqlFunction) {
    kqlFunctionMap.put(kqlFunction.getFunctionName().toUpperCase(), kqlFunction);
  }

}
