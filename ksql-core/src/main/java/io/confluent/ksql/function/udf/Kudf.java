/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udf;

public interface Kudf {

  public void init();

  public Object evaluate(Object... args);
}
