/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.kql.function.udf.math;

import io.confluent.kql.function.KQLFunctionException;
import io.confluent.kql.function.udf.KUDF;

public class Random_KUDF implements KUDF {
  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 0) {
      throw new KQLFunctionException("Random udf should have no input argument.");
    }
    return Math.random();
  }
}
