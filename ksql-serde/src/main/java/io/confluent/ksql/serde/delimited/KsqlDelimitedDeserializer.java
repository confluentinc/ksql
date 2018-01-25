/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.serde.delimited;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;

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
    if (bytes == null) {
      return null;
    }
    String recordCsvString = new String(bytes, StandardCharsets.UTF_8);
    try {
      List<CSVRecord> csvRecords = CSVParser.parse(recordCsvString, CSVFormat.DEFAULT).getRecords();
      if (csvRecords == null || csvRecords.isEmpty()) {
        throw new KsqlException("Deserialization error in the delimited line: " + recordCsvString);
      }
      CSVRecord csvRecord = csvRecords.get(0);
      if (csvRecord == null || csvRecord.size() == 0) {
        throw new KsqlException("Deserialization error in the delimited line: " + recordCsvString);
      }
      List<Object> columns = new ArrayList<>();
      if (csvRecord.size() != schema.fields().size()) {
        throw new KsqlException(
            String.format(
              "Unexpected field count, csvFields:%d schemaFields:%d line: %s",
              csvRecord.size(),
              schema.fields().size(),
              recordCsvString
          )
        );
      }
      for (int i = 0; i < csvRecord.size(); i++) {
        if (csvRecord.get(i) == null) {
          columns.add(null);
        } else {
          columns.add(enforceFieldType(schema.fields().get(i).schema(), csvRecord.get(i)));
        }

      }
      return new GenericRow(columns);
    } catch (Exception e) {
      throw new SerializationException(
          "Exception in deserializing the delimited row: " + recordCsvString,
          e
      );
    }
  }

  private Object enforceFieldType(Schema fieldSchema, String delimitedField) {

    if (delimitedField.isEmpty()) {
      return null;
    }
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
