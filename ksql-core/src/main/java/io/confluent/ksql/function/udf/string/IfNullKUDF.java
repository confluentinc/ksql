/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.KQLFunctionException;
import io.confluent.ksql.function.udf.KUDF;

public class IfNullKUDF implements KUDF {

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
