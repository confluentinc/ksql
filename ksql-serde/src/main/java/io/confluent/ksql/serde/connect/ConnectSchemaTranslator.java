/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.serde.connect;

import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class ConnectSchemaTranslator {
  public Schema toKsqlSchema(final Schema schema) {
    final Schema rowSchema = toKsqlFieldSchema(schema);
    if (rowSchema.type() != Schema.Type.STRUCT) {
      throw new KsqlException("KSQL stream/table schema must be structured");
    }
    return rowSchema;
  }

  protected Schema toKsqlFieldSchema(final Schema schema) {
    switch (schema.type()) {
      case INT32:
      case INT64:
      case FLOAT64:
      case STRING:
      case BOOLEAN:
        return SchemaBuilder.type(schema.type()).build();
      case ARRAY:
        return toKsqlArraySchema(schema);
      case MAP:
        return toKsqlMapSchema(schema);
      case STRUCT:
        return toKsqlStructSchema(schema);
      default:
        throw new KsqlException(
            String.format("Unsupported Schema type: %s", schema.type().getName()));
    }
  }

  private Schema toKsqlMapSchema(final Schema schema) {
    return SchemaBuilder.map(
        toKsqlFieldSchema(schema.keySchema()),
        toKsqlFieldSchema(schema.valueSchema()));
  }

  private Schema toKsqlArraySchema(final Schema schema) {
    return SchemaBuilder.array(schema.valueSchema());
  }

  private Schema toKsqlStructSchema(final Schema schema) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (Field field : schema.fields()) {
      final Schema fieldSchema = toKsqlFieldSchema(field.schema());
      if (fieldSchema != null) {
        schemaBuilder.field(field.name().toUpperCase(), fieldSchema);
      }
    }
    return schemaBuilder.build();
  }
}
