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

package io.confluent.ksql.serde.connect;

import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;

/**
 * Translates KSQL data to and from connect schema conformed data. The connect schema should be
 * translated from ParsedSchema from Schema Registry.
 *
 * <p>The schema may not be compatible with KSQL schema. For example, optional field
 * in KSQL schema might be required in SR schema in which case the translation to connect row will
 * fail.
 */
public class ConnectSRSchemaDataTranslator extends ConnectDataTranslator {

  public ConnectSRSchemaDataTranslator(final Schema schema) {
    super(schema);
  }

  protected void validate(final Schema originalSchema, final Schema connectSchema) {
    if (originalSchema.type() != getSchema().type()) {
      return;
    }
    if (originalSchema.type() != Type.STRUCT) {
      return;
    }
    for (final Field field : originalSchema.fields()) {
      if (!connectSchema.fields().stream().anyMatch(f -> field.name().equals(f.name()))) {
        throw new KsqlException(
            "Schema from Schema Registry misses field with name: " + field.name());
      }
    }
  }

  @Override
  public Object toConnectRow(final Object ksqlData) {
    /*
     * Reconstruct ksqlData struct with given schema and try to put original data in it.
     * Schema may have more fields than ksqlData, don't put those field by default. If needed by
     * some format like Avro, create new subclass to handle
     */
    if (ksqlData instanceof Struct) {
      final Schema schema = getSchema();
      validate(((Struct) ksqlData).schema(), schema);

      final Struct struct = new Struct(schema);
      final Struct source = (Struct) ksqlData;

      for (final Field sourceField : source.schema().fields()) {
        final Object value = source.get(sourceField);
        struct.put(sourceField.name(), value);
      }

      return struct;
    }

    return ksqlData;
  }
}
