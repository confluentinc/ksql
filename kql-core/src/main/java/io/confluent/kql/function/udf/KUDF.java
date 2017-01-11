package io.confluent.kql.function.udf;


public interface KUDF {
  public void init();
  public Object evaluate(Object... args);
}
