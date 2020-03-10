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

package io.confluent.ksql.serde.connect;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectSchemaTranslator {
  private static final Logger log = LoggerFactory.getLogger(ConnectSchemaTranslator.class);

  private static final Map<Type, Function<Schema, Schema>> CONNECT_TO_KSQL =
      ImmutableMap.<Type, Function<Schema, Schema>>builder()
      .put(Type.INT8, s -> Schema.OPTIONAL_INT32_SCHEMA)
      .put(Type.INT16, s -> Schema.OPTIONAL_INT32_SCHEMA)
      .put(Type.INT32, s -> Schema.OPTIONAL_INT32_SCHEMA)
      .put(Type.INT64, s -> Schema.OPTIONAL_INT64_SCHEMA)
      .put(Type.FLOAT32, s -> Schema.OPTIONAL_FLOAT64_SCHEMA)
      .put(Type.FLOAT64, s -> Schema.OPTIONAL_FLOAT64_SCHEMA)
      .put(Type.STRING, s -> Schema.OPTIONAL_STRING_SCHEMA)
      .put(Type.BOOLEAN, s -> Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .put(Type.BYTES, ConnectSchemaTranslator::toKsqlBytesSchema)
      .put(Type.ARRAY, ConnectSchemaTranslator::toKsqlArraySchema)
      .put(Type.MAP, ConnectSchemaTranslator::toKsqlMapSchema)
      .put(Type.STRUCT, ConnectSchemaTranslator::toKsqlStructSchema)
      .build();

  protected static class UnsupportedTypeException extends RuntimeException {
    UnsupportedTypeException(final String error) {
      super(error);
    }
  }

  @SuppressWarnings("MethodMayBeStatic") // Part of injectable API.
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

  private static Schema toKsqlFieldSchema(final Schema schema) {
    final Function<Schema, Schema> handler = CONNECT_TO_KSQL.get(schema.type());
    if (handler == null) {
      throw new UnsupportedTypeException(
          String.format("Unsupported type: %s", schema.type().getName()));
    }

    return handler.apply(schema);
  }

  private static void checkMapKeyType(final Schema.Type type) {
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

  private static Schema toKsqlBytesSchema(final Schema schema) {
    if (DecimalUtil.isDecimal(schema)) {
      return schema;
    }
    throw new UnsupportedTypeException("BYTES type must be DECIMAL schema.");
  }

  private static Schema toKsqlMapSchema(final Schema schema) {
    final Schema keySchema = toKsqlFieldSchema(schema.keySchema());
    checkMapKeyType(keySchema.type());
    return SchemaBuilder.map(
        Schema.OPTIONAL_STRING_SCHEMA,
        toKsqlFieldSchema(schema.valueSchema())
    ).optional().build();
  }

  private static Schema toKsqlArraySchema(final Schema schema) {
    return SchemaBuilder.array(
        toKsqlFieldSchema(schema.valueSchema())
    ).optional().build();
  }

  private static Schema toKsqlStructSchema(final Schema schema) {
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
