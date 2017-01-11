package io.confluent.kql.function.udf.string;

import io.confluent.kql.function.KQLFunctionException;
import io.confluent.kql.function.udf.KUDF;

public class Concat_KUDF implements KUDF {
  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KQLFunctionException("Concat udf should have two input argument.");
    }
    String string = args[0].toString();
    return args[0].toString()+args[1].toString();
  }
}
