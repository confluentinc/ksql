/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.serde.avro;

import io.confluent.ksql.serde.connect.ConnectSRSchemaDataTranslator;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/**
 * Translates KSQL data and schemas to Avro equivalents.
 *
 * <p>Responsible for converting the KSQL schema to a version ready for connect to convert to an
 * avro schema.
 *
 * <p>This includes ensuring field names are valid Avro field names and that nested types do not
 * have name clashes.
 */
public class AvroSRSchemaDataTranslator extends ConnectSRSchemaDataTranslator {

  AvroSRSchemaDataTranslator(final Schema schema) {
    super(schema);
  }

  @Override
  public Object toConnectRow(final Object ksqlData) {
    if (!(ksqlData instanceof Struct)) {
      return ksqlData;
    }
    final Schema schema = getSchema();
    final Struct struct = new Struct(schema);
    final Struct originalData = (Struct) ksqlData;
    final Schema originalSchema = originalData.schema();

    validate(originalSchema, schema);

    for (final Field field : schema.fields()) {
      final Optional<Field> originalField = originalSchema.fields().stream()
          .filter(f -> field.name().equals(f.name())).findFirst();
      if (originalField.isPresent()) {
        struct.put(field, originalData.get(originalField.get()));
      } else {
        if (field.schema().defaultValue() != null || field.schema().isOptional()) {
          struct.put(field, field.schema().defaultValue());
        } else {
          throw new KsqlException("Missing default value for required Avro field: [" + field.name()
              + "]. This field appears in Avro schema in Schema Registry");
        }
      }
    }

    return struct;
  }
}
