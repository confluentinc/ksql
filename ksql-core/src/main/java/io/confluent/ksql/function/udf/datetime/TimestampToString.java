/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udf.datetime;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

public class TimestampToString implements Kudf {

  DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 1) {
      throw new KsqlFunctionException("LCase udf should have one input argument.");
    }
    return dateFormat.format(new Date((long)args[0]));
  }
}
