/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class SchemaUtil {

  public static Schema getTypeSchema(final Schema.Type type) {
    switch (type) {
      case STRING:
        return Schema.STRING_SCHEMA;
      case BOOLEAN:
        return Schema.BOOLEAN_SCHEMA;
      case INT32:
        return Schema.INT32_SCHEMA;
      case INT64:
        return Schema.INT64_SCHEMA;
      case FLOAT64:
        return Schema.FLOAT64_SCHEMA;
      default:
        throw new KQLException("Type is not supported: " + type);
    }
  }

  public static Class getJavaType(final Schema.Type type) {
    switch (type) {
      case STRING:
        return String.class;
      case BOOLEAN:
        return Boolean.class;
      case INT32:
        return Integer.class;
      case INT64:
        return Long.class;
      case FLOAT64:
        return Double.class;
      default:
        throw new KQLException("Type is not supported: " + type);
    }
  }

  public static Schema.Type getTypeSchema(final String kqlType) {

    switch (kqlType) {
      case "STRING":
        return Schema.Type.STRING;
      case "BOOLEAN":
        return Schema.Type.BOOLEAN;
      case "INTEGER":
        return Schema.Type.INT32;
      case "BIGINT":
        return Schema.Type.INT64;
      case "DOUBLE":
        return Schema.Type.FLOAT64;
      default:
        throw new KQLException("Type is not supported: " + kqlType);
    }
  }

  public static Field getFieldByName(final Schema schema, final String fieldName) {
    if (schema.fields() != null) {
      for (Field field : schema.fields()) {
        if (field.name().equals(fieldName)) {
          return field;
        }
      }
    }
    return null;
  }

  public static int getFieldIndexByName(final Schema schema, final String fieldName) {

    if (schema.fields() != null) {
      for (int i = 0; i < schema.fields().size(); i++) {
        Field field = schema.fields().get(i);
        int dotIndex = field.name().indexOf(".");
        if (dotIndex == -1) {
          if (field.name().equals(fieldName)) {
            return i;
          }
        } else {
          if (dotIndex < fieldName.length()) {
            String
                fieldNameWithDot =
                fieldName.substring(0, dotIndex) + "." + fieldName.substring(dotIndex + 1);
            if (field.name().equals(fieldNameWithDot)) {
              return i;
            }
          }
        }

      }
    }
    return -1;
  }

  public static Schema buildSchemaWithAlias(final Schema schema, final String alias) {
    SchemaBuilder newSchema = SchemaBuilder.struct().name(schema.name());
    for (Field field : schema.fields()) {
      newSchema.field((alias + "." + field.name()), field.schema());
    }
    return newSchema;
  }

  public static final ImmutableMap<String, String> TYPE_MAP =
      new ImmutableMap.Builder<String, String>()
          .put("STRING", "VARCHAR")
          .put("INT64", "BIGINT")
          .put("INT32", "INTEGER")
          .put("FLOAT64", "DOUBLE")
          .put("BOOLEAN", "BOOLEAN")
          .build();

}
