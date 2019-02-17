/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.serde.delimited;

import io.confluent.common.logging.StructuredLogger;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.processing.log.ProcessingLogContext;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
import io.confluent.ksql.util.KsqlException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;

public class KsqlDelimitedDeserializer implements Deserializer<GenericRow> {

  private final Schema schema;
  private final StructuredLogger recordLogger;
  private final ProcessingLogContext processingLogContext;

  KsqlDelimitedDeserializer(
      final Schema schema,
      final StructuredLogger recordLogger,
      final ProcessingLogContext processingLogContext) {
    this.schema = Objects.requireNonNull(schema);
    this.recordLogger = Objects.requireNonNull(recordLogger);
    this.processingLogContext = Objects.requireNonNull(processingLogContext);
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    final String recordCsvString = new String(bytes, StandardCharsets.UTF_8);
    try {
      final List<CSVRecord> csvRecords = CSVParser.parse(recordCsvString, CSVFormat.DEFAULT)
          .getRecords();

      if (csvRecords == null || csvRecords.isEmpty()) {
        throw new KsqlException("Deserialization error in the delimited line: " + recordCsvString);
      }
      final CSVRecord csvRecord = csvRecords.get(0);
      if (csvRecord == null || csvRecord.size() == 0) {
        throw new KsqlException("Deserialization error in the delimited line: " + recordCsvString);
      }
      final List<Object> columns = new ArrayList<>();
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
    } catch (final Exception e) {
      recordLogger.error(
          SerdeProcessingLogMessageFactory.deserializationErrorMsg(
              e,
              Optional.ofNullable(bytes),
              processingLogContext.getConfig()));
      throw new SerializationException(
          "Exception in deserializing the delimited row: " + recordCsvString,
          e
      );
    }
  }

  private Object enforceFieldType(final Schema fieldSchema, final String delimitedField) {

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
