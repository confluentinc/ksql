/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.kql.function.udf.string;

import io.confluent.kql.function.KQLFunctionException;
import io.confluent.kql.function.udf.KUDF;

public class Substring_KUDF implements KUDF {

  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if ((args.length < 2) || (args.length > 3)) {
      throw new KQLFunctionException("Substring udf should have two or three input argument.");
    }
    String string = args[0].toString();
    long start = (Long) args[1];
    if (args.length == 2) {
      return string.substring((int)start);
    } else {
      long end = (Long) args[2];
      return string.substring((int)start, (int)end);
    }
  }
}
