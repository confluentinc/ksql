/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.serde.csv;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.util.KQLException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KQLCsvDeserializer implements Deserializer<GenericRow> {

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {
    String recordCsvString = new String(bytes);
    try {
      CSVRecord csvRecord = CSVParser.parse(recordCsvString, CSVFormat.DEFAULT).getRecords().get(0);
      List<Object> columns = new ArrayList();
      for (int i = 0; i < csvRecord.size(); i++) {
        columns.add(csvRecord.get(i));
      }
      return new GenericRow(columns);
    } catch (IOException e) {
      throw new KQLException("Could not parse the CSV record: " + recordCsvString);
    }
  }

  @Override
  public void close() {

  }
}
