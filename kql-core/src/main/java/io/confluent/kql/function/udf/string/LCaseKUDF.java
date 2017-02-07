/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.function.udf.string;

import io.confluent.kql.function.KQLFunctionException;
import io.confluent.kql.function.udf.KUDF;

public class LCaseKUDF implements KUDF {

  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 1) {
      throw new KQLFunctionException("LCase udf should have one input argument.");
    }
    return args[0].toString().toLowerCase();
  }
}
