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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectSchemaTranslator {
  private static final Logger log = LoggerFactory.getLogger(ConnectSchemaTranslator.class);

  protected static class UnsupportedTypeException extends RuntimeException {
    public UnsupportedTypeException(final String error) {
      super(error);
    }
  }

  public Schema toKsqlSchema(final Schema schema) {
    try {
      final Schema rowSchema = toKsqlFieldSchema(schema);
      if (rowSchema.type() != Schema.Type.STRUCT) {
        throw new KsqlException("KSQL stream/table schema must be structured");
      }
      return rowSchema;
    } catch (final UnsupportedTypeException e) {
      throw new KsqlException("Unsupported type at root of schema: " + e.getMessage(), e);
    }
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  protected Schema toKsqlFieldSchema(final Schema schema) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    switch (schema.type()) {
      case INT8:
      case INT16:
      case INT32:
        return Schema.OPTIONAL_INT32_SCHEMA;
      case FLOAT32:
      case FLOAT64:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      case INT64:
      case STRING:
      case BOOLEAN:
        return SchemaBuilder.type(schema.type()).optional().build();
      case ARRAY:
        return toKsqlArraySchema(schema);
      case MAP:
        return toKsqlMapSchema(schema);
      case STRUCT:
        return toKsqlStructSchema(schema);
      default:
        throw new UnsupportedTypeException(
            String.format("Unsupported type: %s", schema.type().getName()));
    }
  }

  private void checkMapKeyType(final Schema.Type type) {
    switch (type) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case BOOLEAN:
      case STRING:
        return;
      default:
        throw new UnsupportedTypeException("Unsupported type for map key: " + type.getName());
    }
  }

  private Schema toKsqlMapSchema(final Schema schema) {
    final Schema keySchema = toKsqlFieldSchema(schema.keySchema());
    checkMapKeyType(keySchema.type());
    return SchemaBuilder.map(
        Schema.OPTIONAL_STRING_SCHEMA,
        toKsqlFieldSchema(schema.valueSchema())
    ).optional().build();
  }

  private Schema toKsqlArraySchema(final Schema schema) {
    return SchemaBuilder.array(
        toKsqlFieldSchema(schema.valueSchema())
    ).optional().build();
  }

  private Schema toKsqlStructSchema(final Schema schema) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (final Field field : schema.fields()) {
      try {
        final Schema fieldSchema = toKsqlFieldSchema(field.schema());
        schemaBuilder.field(field.name().toUpperCase(), fieldSchema);
      } catch (final UnsupportedTypeException e) {
        log.error("Error inferring schema at field {}: {}", field.name(), e.getMessage());
      }
    }
    return schemaBuilder.optional().build();
  }
}
