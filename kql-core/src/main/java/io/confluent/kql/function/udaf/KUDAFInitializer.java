package io.confluent.kql.function.udaf;


import org.apache.kafka.streams.kstream.Initializer;

import java.util.ArrayList;
import java.util.List;

import io.confluent.kql.physical.GenericRow;

public class KUDAFInitializer implements Initializer<GenericRow>{

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
