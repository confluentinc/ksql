/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

public class CeilKudf implements Kudf {

  @Override
  public void init() {
  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 1) {
      throw new KsqlFunctionException("Ceil udf should have one input argument.");
    }
    return Math.ceil((Double) args[0]);
  }
}
