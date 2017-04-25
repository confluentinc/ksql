/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.HashMap;

public class SchemaUtil {


  public static Class getJavaType(final Schema schema) {
    switch (schema.type()) {
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
      case ARRAY:
        Class elementClass = getJavaType(schema.valueSchema());
        return java.lang.reflect.Array.newInstance(elementClass, 0).getClass();
      case MAP:
        return (new HashMap<>()).getClass();
      default:
        throw new KQLException("Type is not supported: " + schema.type());
    }
  }


  public static Schema getTypeSchema(final String kqlType) {
    switch (kqlType) {
      case "STRING":
        return Schema.STRING_SCHEMA;
      case "BOOLEAN":
        return Schema.BOOLEAN_SCHEMA;
      case "INTEGER":
        return Schema.INT32_SCHEMA;
      case "BIGINT":
        return Schema.INT64_SCHEMA;
      case "DOUBLE":
        return Schema.FLOAT64_SCHEMA;
      case "ARRAY":
        return SchemaBuilder.array(getTypeSchema(kqlType.substring("ARRAY".length() + 1, kqlType
                                                                                             .length() - 1).trim()));
      case "MAP":
        return SchemaBuilder.map(Schema.STRING_SCHEMA, getTypeSchema(kqlType.substring(kqlType
                                                                                           .indexOf(",") + 1, kqlType.length() - 1).trim()));
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
          .put("ARRAY", "ARRAY")
          .put("MAP", "MAP")
          .build();

  public static String getJavaCastString(Schema schema) {
    switch (schema.type()) {
      case INT32:
        return "(Integer)";
      case INT64:
        return "(Long)";
      case FLOAT64:
        return "(Double)";
      case STRING:
        return "(String)";
      case BOOLEAN:
        return "(Boolean)";
      default:
        //TODO: Add complex types later!
        return "";
    }
  }

}
