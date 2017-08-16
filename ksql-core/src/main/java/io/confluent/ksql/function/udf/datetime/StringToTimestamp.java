/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.function.udf.datetime;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

public class StringToTimestamp implements Kudf {

  DateFormat dateFormat = null;

  @Override
  public void init() {
  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KsqlFunctionException("StringToTimestamp udf should have two input argument:"
                                      + " date value and format.");
    }
    try {
      if(dateFormat == null) {
        dateFormat = new SimpleDateFormat(args[1].toString());
      }
      return dateFormat.parse(args[0].toString()).getTime();
    } catch (ParseException e) {
      throw new KsqlFunctionException("Exception running StringToTimestamp(" + args[0] +" , " +
                                      args[1] + ") : " + e.getMessage(), e);
    }
  }
}
