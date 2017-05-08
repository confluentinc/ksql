/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function.udaf;

import io.confluent.ksql.physical.GenericRow;
import org.apache.kafka.streams.kstream.Initializer;

import java.util.ArrayList;
import java.util.List;

public class KUDAFInitializer implements Initializer<GenericRow> {

  final List initialGenericRowColumns;

  public KUDAFInitializer(List initialGenericRowColumns) {
    this.initialGenericRowColumns = initialGenericRowColumns;
  }

  @Override
  public GenericRow apply() {
    List rowColumns = new ArrayList();
    for (Object obj: initialGenericRowColumns) {
      rowColumns.add(obj);
    }
    return new GenericRow(rowColumns);
  }

}
