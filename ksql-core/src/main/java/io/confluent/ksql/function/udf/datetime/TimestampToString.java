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

  DateFormat dateFormat = null;
  @Override
  public void init() {

  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KsqlFunctionException("TimestampToString udf should have two input argument:"
                                      + " date value and format.");
    }
    try {
      if(dateFormat == null) {
        dateFormat = new SimpleDateFormat(args[1].toString());
      }
      return dateFormat.format(new Date((long)args[0]));
    } catch (Exception e) {
      throw new KsqlFunctionException("Exception running TimestampToString(" + args[0] +" , " +
                                      args[1] + ") : " + e.getMessage(), e);
    }
  }
}
