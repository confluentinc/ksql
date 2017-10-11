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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KsqlDelimitedDeserializer implements Deserializer<GenericRow> {

  private static final Logger log = LoggerFactory.getLogger(KsqlDelimitedDeserializer.class);
  private static ConfigDef configDef;
  private final Schema schema;
  private Boolean failOnDeserializationError = Boolean.FALSE;


  public KsqlDelimitedDeserializer(Schema schema) {
    this.schema = schema;
  }

  static {
    configDef = new ConfigDef()
        .define(KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.MEDIUM,
            "Whether or not KSQL should fail when there are deserialization errors." +
                "The default is false, errors will be logged");
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
    final Map<String, Object> config = configDef.parse(map);
    failOnDeserializationError = (Boolean) config.getOrDefault(
        KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG,
        Boolean.FALSE);
  }

  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    String recordCsvString = new String(bytes);
    try {
      List<CSVRecord> csvRecords = CSVParser.parse(recordCsvString, CSVFormat.DEFAULT)
          .getRecords();
      if (csvRecords == null || csvRecords.isEmpty()) {
        throw new KsqlException("Deserialization error in the delimited line: " + recordCsvString);
      }
      CSVRecord csvRecord = csvRecords.get(0);
      if (csvRecord == null || csvRecord.size() == 0) {
        throw new KsqlException("Deserialization error in the delimited line: " + recordCsvString);
      }
      List<Object> columns = new ArrayList();
      if (csvRecord.size() != schema.fields().size()) {
        throw new KsqlException("Missing/Extra fields in the delimited line: " + recordCsvString);
      }
      for (int i = 0; i < csvRecord.size(); i++) {
        columns.add(enforceFieldType(schema.fields().get(i).schema(), csvRecord.get(i)));
      }
      return new GenericRow(columns);
    } catch (Exception e) {
      if (failOnDeserializationError) {
        throw new SerializationException("Exception in deserializing the delimited row: " + recordCsvString,
            e);
      }
      log.warn("KsqlDelimitedDeserializer failed to deserialize data for topic: {}", topic, e);
      return null;
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
