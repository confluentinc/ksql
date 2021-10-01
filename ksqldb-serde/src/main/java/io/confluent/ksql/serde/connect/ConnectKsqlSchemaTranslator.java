/*
 * Copyright 2020 Confluent Inc.
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
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter.Option;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConnectKsqlSchemaTranslator {

  private static final Logger log = LoggerFactory.getLogger(ConnectSchemaTranslator.class);

  private static final ConnectSchemaTranslationPolicies DEFAULT_POLICY =
      ConnectSchemaTranslationPolicies.of(ConnectSchemaTranslationPolicy.UPPERCASE_FIELD_NAME);

  private static final SqlSchemaFormatter FORMATTER =
      new SqlSchemaFormatter(w -> false, Option.AS_COLUMN_LIST);

  private final Map<Type, BiFunction<ConnectKsqlSchemaTranslator, Schema, Schema>> connectToSql =
      ImmutableMap.<Type, BiFunction<ConnectKsqlSchemaTranslator, Schema, Schema>>builder()
          .put(Type.INT8, (instance, s) -> Schema.OPTIONAL_INT32_SCHEMA)
          .put(Type.INT16, (instance, s) -> Schema.OPTIONAL_INT32_SCHEMA)
          .put(Type.INT32, (instance, s) -> this.convertInt32Schema(s))
          .put(Type.INT64, (instance, s) -> instance.convertInt64Schema(s))
          .put(Type.FLOAT32, (instance, s) -> Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put(Type.FLOAT64, (instance, s) -> Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put(Type.STRING, (instance, s) -> Schema.OPTIONAL_STRING_SCHEMA)
          .put(Type.BOOLEAN, (instance, s) -> Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .put(Type.BYTES, (instance, s) -> instance.toKsqlBytesSchema(s))
          .put(Type.ARRAY, (instance, s) -> instance.toKsqlArraySchema(s))
          .put(Type.MAP, (instance, s) -> instance.toKsqlMapSchema(s))
          .put(Type.STRUCT, (instance, s) -> instance.toKsqlStructSchema(s))
          .build();

  public static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().build();
  public static final Schema OPTIONAL_TIME_SCHEMA = Time.builder().optional().build();
  public static final Schema OPTIONAL_DATE_SCHEMA = Date.builder().optional().build();

  private final ConnectSchemaTranslationPolicies policies;

  public ConnectKsqlSchemaTranslator() {
    this(DEFAULT_POLICY);
  }

  public ConnectKsqlSchemaTranslator(ConnectSchemaTranslationPolicies policies) {
    this.policies = policies;
  }

  /**
   * Ensures all schema types are optional.
   *
   * @param schema the source schema.
   * @return the ksql compatible schema.
   */
  public Schema toKsqlSchema(final Schema schema) {
    try {
      final Schema rowSchema = toKsqlFieldSchema(schema);
      if (rowSchema.type() != Schema.Type.STRUCT) {
        throw new KsqlException("KSQL stream/table schema must be structured");
      }

      if (rowSchema.fields().isEmpty()) {
        throw new KsqlException("Schema does not include any columns with "
            + "types that ksqlDB supports."
            + System.lineSeparator()
            + "schema: " + FORMATTER.format(schema));
      }

      return rowSchema;
    } catch (final UnsupportedTypeException e) {
      throw new KsqlException("Unsupported type at root of schema: " + e.getMessage(), e);
    }
  }

  private Schema toKsqlFieldSchema(final Schema schema) {
    final BiFunction<ConnectKsqlSchemaTranslator, Schema, Schema> handler = connectToSql.get(
        schema.type());
    if (handler == null) {
      throw new UnsupportedTypeException(
          String.format("Unsupported type: %s", schema.type().getName()));
    }

    return handler.apply(this, schema);
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

  private Schema convertInt64Schema(final Schema schema) {
    if (schema.name() == Timestamp.LOGICAL_NAME) {
      return OPTIONAL_TIMESTAMP_SCHEMA;
    } else {
      return Schema.OPTIONAL_INT64_SCHEMA;
    }
  }

  private Schema convertInt32Schema(final Schema schema) {
    if (schema.name() == Time.LOGICAL_NAME) {
      return OPTIONAL_TIME_SCHEMA;
    } else if (schema.name() == Date.LOGICAL_NAME) {
      return OPTIONAL_DATE_SCHEMA;
    } else {
      return Schema.OPTIONAL_INT32_SCHEMA;
    }
  }

  private Schema toKsqlBytesSchema(final Schema schema) {
    if (DecimalUtil.isDecimal(schema)) {
      return schema;
    } else {
      return Schema.OPTIONAL_BYTES_SCHEMA;
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
        if (policies.enabled(ConnectSchemaTranslationPolicy.UPPERCASE_FIELD_NAME)) {
          schemaBuilder.field(field.name().toUpperCase(), fieldSchema);
        } else if (policies.enabled(ConnectSchemaTranslationPolicy.LOWERCASE_FIELD_NAME)) {
          schemaBuilder.field(field.name().toLowerCase(), fieldSchema);
        } else if (policies.enabled(ConnectSchemaTranslationPolicy.ORIGINAL_FIELD_NAME)) {
          schemaBuilder.field(field.name(), fieldSchema);
        } else {
          // Default to uppercase
          schemaBuilder.field(field.name().toUpperCase(), fieldSchema);
        }
      } catch (final UnsupportedTypeException e) {
        log.error("Error inferring schema at field {}: {}", field.name(), e.getMessage());
      }
    }
    return schemaBuilder.optional().build();
  }

  private static class UnsupportedTypeException extends RuntimeException {

    UnsupportedTypeException(final String error) {
      super(error);
    }
  }
}
