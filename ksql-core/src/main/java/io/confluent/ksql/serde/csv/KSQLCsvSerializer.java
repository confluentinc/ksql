/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.serde.csv;

import io.confluent.ksql.physical.GenericRow;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class KSQLCsvSerializer implements Serializer<GenericRow> {


  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public byte[] serialize(final String topic, final GenericRow genericRow) {
    StringBuilder recordString = new StringBuilder();
    for (int i = 0; i < genericRow.getColumns().size(); i++) {
      if (i != 0) {
        recordString.append(",");
      }
      recordString.append(genericRow.columns.get(i).toString());
    }
    return recordString.toString().getBytes();
  }

  @Override
  public void close() {

  }
}
