/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util.timestamp;

import io.confluent.ksql.util.KSQLException;

public class TimespampExtractorFactory {

  public synchronized static Class getTimestampExtractorClass(int columnIndex) {
    switch (columnIndex) {
      case 0:
        return Column0.class;
      case 1:
        return Column1.class;
      case 2:
        return Column2.class;
      case 3:
        return Column3.class;
      case 4:
        return Column4.class;
      case 5:
        return Column5.class;
      default:
        throw new KSQLException("Column with index greater than 5 cannot be used as timestamp.");
    }
  }

}
