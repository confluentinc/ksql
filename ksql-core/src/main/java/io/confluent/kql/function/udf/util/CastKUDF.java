/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.function.udf.util;

import io.confluent.kql.function.KQLFunctionException;
import io.confluent.kql.function.udf.KUDF;

public class CastKUDF implements KUDF {

  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KQLFunctionException("Concat udf should have two input argument.");
    }
    String string = args[1].toString();

    return args[0].toString() + args[1].toString();
  }
}
