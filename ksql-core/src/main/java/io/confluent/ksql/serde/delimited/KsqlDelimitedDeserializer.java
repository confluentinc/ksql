/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.serde.delimited;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class KsqlDelimitedDeserializer implements Deserializer<GenericRow> {

  private final Schema schema;

  public KsqlDelimitedDeserializer(Schema schema) {
    this.schema = schema;
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {
    String recordCsvString = new String(bytes);
    try {
      CSVRecord csvRecord = CSVParser.parse(recordCsvString, CSVFormat.DEFAULT).getRecords().get(0);
      List<Object> columns = new ArrayList();
      if (csvRecord.size() != schema.fields().size()) {
        throw new KsqlException("Missing/Extra fields in the delimited line: " + recordCsvString);
      }
      for (int i = 0; i < csvRecord.size(); i++) {
        columns.add(enforceFieldType(schema.fields().get(i).schema(), csvRecord.get(i)));
      }
      return new GenericRow(columns);
    } catch (IOException e) {
      throw new KsqlException("Could not parse the DELIMITED record: " + recordCsvString, e);
    }
  }

  private Object enforceFieldType(Schema fieldSchema, String delimitedField) {

    switch (fieldSchema.type()) {
      case BOOLEAN:
        return Boolean.parseBoolean(delimitedField);
      case INT32:
        return Integer.parseInt(delimitedField);
      case INT64:
        return Long.parseLong(delimitedField);
      case FLOAT64:
        return Double.parseDouble(delimitedField);
      case STRING:
        return delimitedField;
      case ARRAY:
      case MAP:
      default:
        throw new KsqlException("Type is not supported: " + fieldSchema.type());
    }
  }

  @Override
  public void close() {

  }
}
