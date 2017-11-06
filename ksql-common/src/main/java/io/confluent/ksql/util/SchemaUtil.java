/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SchemaUtil {

  public static final String ARRAY = "ARRAY";
  public static final String MAP = "MAP";

  public static final String ROWKEY_NAME = "ROWKEY";
  public static final String ROWTIME_NAME = "ROWTIME";
  public static final int ROWKEY_NAME_INDEX = 1;
  public static final int ROWTIME_NAME_INDEX = 0;

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
        return HashMap.class;
      default:
        throw new KsqlException("Type is not supported: " + schema.type());
    }
  }


  public static Optional<Field> getFieldByName(final Schema schema, final String fieldName) {
    if (schema.fields() != null) {
      for (Field field : schema.fields()) {
        if (field.name().equals(fieldName)) {
          return Optional.of(field);
        } else if (field.name().equals(fieldName.substring(fieldName.indexOf(".") + 1))) {
          return Optional.of(field);
        }
      }
    }
    return Optional.empty();
  }

  public static Schema getTypeSchema(final String sqlType) {
    switch (sqlType) {
      case "VARCHAR":
      case "STRING":
        return Schema.STRING_SCHEMA;
      case "BOOLEAN":
      case "BOOL":
        return Schema.BOOLEAN_SCHEMA;
      case "INTEGER":
      case "INT":
        return Schema.INT32_SCHEMA;
      case "BIGINT":
      case "LONG":
        return Schema.INT64_SCHEMA;
      case "DOUBLE":
        return Schema.FLOAT64_SCHEMA;
      default:
        return getKsqlComplexType(sqlType);
    }
  }

  private static Schema getKsqlComplexType(final String sqlType) {
    if (sqlType.startsWith(ARRAY)) {
      return SchemaBuilder.array(getTypeSchema(sqlType.substring(ARRAY.length() + 1, sqlType.length() - 1)));
    } else if (sqlType.startsWith(MAP)) {
      //TODO: For now only primitive data types for map are supported. Will have to add nested types.
      String[] mapTypesStrs = sqlType.substring("MAP".length() + 1, sqlType.length() - 1)
              .trim().split(",");
      if (mapTypesStrs.length != 2) {
        throw new KsqlException("Map type is not defined correctly.: " + sqlType);
      }
      String keyType = mapTypesStrs[0].trim();
      String valueType = mapTypesStrs[1].trim();
      return SchemaBuilder.map(getTypeSchema(keyType), getTypeSchema(valueType));
    }
    throw new KsqlException("Unsupported type: " + sqlType);
  }



  public static int getFieldIndexByName(final Schema schema, final String fieldName) {
    if (schema.fields() == null) {
      return -1;
    }
    for (int i = 0; i < schema.fields().size(); i++) {
      Field field = schema.fields().get(i);
      int dotIndex = field.name().indexOf('.');
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
          .put("STRING", "VARCHAR(STRING)")
          .put("INT64", "BIGINT")
          .put("INT32", "INTEGER")
          .put("FLOAT64", "DOUBLE")
          .put("BOOLEAN", "BOOLEAN")
          .put("ARRAY", "ARRAY")
          .put("MAP", "MAP")
          .build();

  public static String getSchemaFieldName(Field field) {
    if (field.schema().type() == Schema.Type.ARRAY) {
      return "ARRAY[" + TYPE_MAP.get(field.schema().valueSchema().type().name()) + "]";
    } else if (field.schema().type() == Schema.Type.MAP) {
      return "MAP[" + TYPE_MAP.get(field.schema().keySchema().type().name()) + "," +
             TYPE_MAP.get(field.schema().valueSchema().type().name()) + "]";
    } else {
      return TYPE_MAP.get(field.schema().type().name());
    }
  }

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

  public static synchronized Schema addImplicitRowTimeRowKeyToSchema(Schema schema) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.field(SchemaUtil.ROWTIME_NAME, Schema.INT64_SCHEMA);
    schemaBuilder.field(SchemaUtil.ROWKEY_NAME, Schema.STRING_SCHEMA);
    for (Field field: schema.fields()) {
      if (!field.name().equals(SchemaUtil.ROWKEY_NAME) && !field.name().equals(SchemaUtil
                                                                                   .ROWTIME_NAME)) {
        schemaBuilder.field(field.name(), field.schema());
      }
    }
    return schemaBuilder.build();
  }

  public static synchronized Schema removeImplicitRowTimeRowKeyFromSchema(Schema schema) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (Field field: schema.fields()) {
      String fieldName = field.name();
      fieldName = fieldName.substring(fieldName.indexOf('.') + 1);
      if (!fieldName.equalsIgnoreCase(SchemaUtil.ROWTIME_NAME)
          && !fieldName.equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
        schemaBuilder.field(fieldName, field.schema());
      }
    }
    return schemaBuilder.build();
  }

  public static synchronized Set<Integer> getRowTimeRowKeyIndexes(Schema schema) {
    Set indexSet = new HashSet();
    for (int i = 0; i < schema.fields().size(); i++) {
      Field field = schema.fields().get(i);
      if (field.name().equalsIgnoreCase(SchemaUtil.ROWTIME_NAME)
          || field.name().equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
        indexSet.add(i);
      }
    }
    return indexSet;
  }

  public static synchronized String getSchemaDefinitionString(Schema schema) {
    StringBuilder stringBuilder = new StringBuilder("[");
    boolean addComma = false;
    for (Field field : schema.fields()) {
      if (addComma) {
        stringBuilder.append(" , ");
      } else {
        addComma = true;
      }
      stringBuilder.append(field.name() + " : " + field.schema().type());
    }
    stringBuilder.append("]");
    return stringBuilder.toString();
  }

}
