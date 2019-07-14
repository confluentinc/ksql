/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.serde.delimited;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;

public class KsqlDelimitedDeserializer implements Deserializer<Object> {

  private static final Map<Type, Function<String, Object>> PARSERS = ImmutableMap.of(
      Type.BOOLEAN, Boolean::parseBoolean,
      Type.INT32, Integer::parseInt,
      Type.INT64, Long::parseLong,
      Type.FLOAT64, Double::parseDouble,
      Type.STRING, s -> s
  );

  private final ConnectSchema schema;

  KsqlDelimitedDeserializer(
      final PersistenceSchema schema
  ) {
    this.schema = Objects.requireNonNull(schema, "schema").getConnectSchema();

    throwOnUnsupported(this.schema);
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public Struct deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    try {
      final String recordCsvString = new String(bytes, StandardCharsets.UTF_8);
      final List<CSVRecord> csvRecords = CSVParser.parse(recordCsvString, CSVFormat.DEFAULT)
          .getRecords();

      if (csvRecords.isEmpty()) {
        throw new KsqlException("No fields in record");
      }

      final CSVRecord csvRecord = csvRecords.get(0);
      if (csvRecord == null || csvRecord.size() == 0) {
        throw new KsqlException("No fields in record.");
      }

      if (csvRecord.size() != schema.fields().size()) {
        throw new KsqlException(
            String.format(
                "Unexpected field count, csvFields:%d schemaFields:%d",
              csvRecord.size(),
                schema.fields().size()
          )
        );
      }

      final Struct struct = new Struct(schema);

      final Iterator<Field> it = schema.fields().iterator();

      for (int i = 0; i < csvRecord.size(); i++) {
        final Field field = it.next();
        if (csvRecord.get(i) == null) {
          struct.put(field, null);
        } else {
          final Object coerced = enforceFieldType(field.schema(), csvRecord.get(i));
          struct.put(field, coerced);
        }

      }
      return struct;
    } catch (final Exception e) {
      throw new SerializationException("Error deserializing delimited row", e);
    }
  }

  @Override
  public void close() {
  }

  private static Object enforceFieldType(
      final Schema fieldSchema,
      final String delimitedField
  ) {
    if (delimitedField.isEmpty()) {
      return null;
    }

    if (DecimalUtil.isDecimal(fieldSchema)) {
      return DecimalUtil.ensureFit(new BigDecimal(delimitedField),fieldSchema);
    }

    final Function<String, Object> parser = PARSERS.get(fieldSchema.type());
    if (parser == null) {
      throw new KsqlException("Type is not supported: " + fieldSchema.type());
    }

    return parser.apply(delimitedField);
  }

  private static void throwOnUnsupported(final Schema schema) {
    if (schema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("DELIMITED expects all top level schemas to be STRUCTs");
    }

    schema.fields().forEach(field -> {
      final Type type = field.schema().type();
      if (!PARSERS.keySet().contains(type) && !DecimalUtil.isDecimal(field.schema())) {
        throw new UnsupportedOperationException(
            "DELIMITED does not support type: " + type + ", field: " + field.name());
      }
    });
  }
}
