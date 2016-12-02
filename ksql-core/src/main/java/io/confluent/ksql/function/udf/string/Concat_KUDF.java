package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.KSQLFunctionException;
import io.confluent.ksql.function.udf.KUDF;

public class Concat_KUDF implements KUDF {
  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KSQLFunctionException("Concat udf should have two input argument.");
    }
    String string = args[0].toString();
    return args[0].toString()+args[1].toString();
  }
}
