/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.kql.function.udf.string;

import io.confluent.kql.function.KQLFunctionException;
import io.confluent.kql.function.udf.KUDF;

public class IfNull_KUDF implements KUDF {
  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KQLFunctionException("IfNull udf should have two input argument.");
    }
    if (args[0] == null) {
      return args[1];
    } else {
      return args[0];
    }
  }
}
