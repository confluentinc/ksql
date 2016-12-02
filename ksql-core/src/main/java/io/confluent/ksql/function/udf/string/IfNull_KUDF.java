package io.confluent.ksql.function.udf.string;


import io.confluent.ksql.function.KSQLFunctionException;
import io.confluent.ksql.function.udf.KUDF;

public class IfNull_KUDF implements KUDF {
  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KSQLFunctionException("IfNull udf should have two input argument.");
    }
    if (args[0] == null) {
      return args[1];
    } else {
      return args[0];
    }
  }
}
