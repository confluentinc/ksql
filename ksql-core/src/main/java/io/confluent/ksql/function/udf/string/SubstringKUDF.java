/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.KSQLFunctionException;
import io.confluent.ksql.function.udf.KUDF;

public class SubstringKUDF implements KUDF {

  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if ((args.length < 2) || (args.length > 3)) {
      throw new KSQLFunctionException("Substring udf should have two or three input argument.");
    }
    String string = args[0].toString();
    long start = (Long) args[1];
    if (args.length == 2) {
      return string.substring((int) start);
    } else {
      long end = (Long) args[2];
      return string.substring((int) start, (int) end);
    }
  }
}
