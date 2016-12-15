package io.confluent.ksql.util;


import com.google.common.collect.ImmutableMap;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class SchemaUtil {

  public static Schema getTypeSchema(Schema.Type type) {
    if (type == Schema.Type.BOOLEAN) {
      return Schema.BOOLEAN_SCHEMA;
    } else if (type == Schema.Type.INT32) {
      return Schema.INT32_SCHEMA;
    } else if (type == Schema.Type.INT64) {
      return Schema.INT64_SCHEMA;
    } else if (type == Schema.Type.FLOAT64) {
      return Schema.FLOAT64_SCHEMA;
    } else if (type == Schema.Type.STRING) {
      return Schema.STRING_SCHEMA;
    }
    throw new KSQLException("Type is not supported: " + type);
  }

  public static Class getJavaType(Schema.Type type) {
    if (type == Schema.Type.BOOLEAN) {
      return Boolean.class;
    } else if (type == Schema.Type.INT32) {
      return Integer.class;
    } else if (type == Schema.Type.INT64) {
      return Long.class;
    } else if (type == Schema.Type.FLOAT64) {
      return Double.class;
    } else if (type == Schema.Type.STRING) {
      return String.class;
    }
    throw new KSQLException("Type is not supported: " + type);
  }

  public static Schema.Type getTypeSchema(String ksqlType) {

    if (ksqlType.equalsIgnoreCase("STRING")) {
      return Schema.Type.STRING;
    } else if (ksqlType.equalsIgnoreCase("INTEGER")) {
      return Schema.Type.INT32;
    } else if (ksqlType.equalsIgnoreCase("DOUBLE")) {
      return Schema.Type.FLOAT64;
    } else if (ksqlType.equalsIgnoreCase("BIGINT")) {
      return Schema.Type.INT64;
    } else if (ksqlType.equalsIgnoreCase("BOOLEAN")) {
      return Schema.Type.BOOLEAN;
    }
    throw new KSQLException("Type is not supported: " + ksqlType);
  }

  public static Field getFieldByName(Schema schema, String fieldName) {
    fieldName = fieldName.toUpperCase();

    if (schema.fields() != null) {
      
      for (Field field : schema.fields()) {
        if (field.name().equalsIgnoreCase(fieldName)) {
          return field;
        }
      }
    }
    return null;
  }

  public static int getFieldIndexByName(Schema schema, String fieldName) {

    if (schema.fields() != null) {
      for (int i = 0; i < schema.fields().size(); i++) {
        Field field = schema.fields().get(i);
        int dotIndex = field.name().indexOf(".");
        if (dotIndex == -1) {
          if (field.name().equalsIgnoreCase(fieldName)) {
            return i;
          }
        } else {
          if (dotIndex < fieldName.length()) {
            String
                fieldNameWithDot =
                fieldName.substring(0, dotIndex) + "." + fieldName.substring(dotIndex + 1);
            if (field.name().equalsIgnoreCase(fieldNameWithDot)) {
              return i;
            }
          }
        }

      }
    }
    return -1;
  }

  public static Schema buildSchemaWithAlias(Schema schema, String alias) {
    SchemaBuilder newSchema = SchemaBuilder.struct().name(schema.name());
    for (Field field : schema.fields()) {
      newSchema.field((alias + "." + field.name()).toUpperCase(), field.schema());
    }
    return newSchema;
  }

  public static final ImmutableMap<String, String>
      typeMap =
      new ImmutableMap.Builder<String, String>()
          .put("STRING", "VARCHAR")
          .put("INT64", "BIGINT")
          .put("INT32", "INTEGER")
          .put("FLOAT64", "DOUBLE")
          .put("BOOLEAN", "BOOLEAN")
          .build();

}
