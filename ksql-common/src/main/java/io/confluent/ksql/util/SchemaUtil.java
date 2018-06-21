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
import org.apache.kafka.connect.data.Struct;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.create;
import static org.apache.avro.Schema.createArray;
import static org.apache.avro.Schema.createMap;
import static org.apache.avro.Schema.createUnion;

public class SchemaUtil {

  public static final String ARRAY = "ARRAY";
  public static final String MAP = "MAP";
  public static final String STRUCT = "STRUCT";

  public static final String ROWKEY_NAME = "ROWKEY";
  public static final String ROWTIME_NAME = "ROWTIME";
  public static final int ROWKEY_NAME_INDEX = 1;
  private static final Map<Type, Supplier<Schema>> typeToSchema
      = ImmutableMap.<Type, Supplier<Schema>>builder()
      .put(String.class, () -> Schema.OPTIONAL_STRING_SCHEMA)
      .put(boolean.class, () -> Schema.BOOLEAN_SCHEMA)
      .put(Boolean.class, () -> Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .put(Integer.class, () -> Schema.OPTIONAL_INT32_SCHEMA)
      .put(int.class, () -> Schema.INT32_SCHEMA)
      .put(Long.class, () -> Schema.OPTIONAL_INT64_SCHEMA)
      .put(long.class, () -> Schema.INT64_SCHEMA)
      .put(Double.class, () -> Schema.OPTIONAL_FLOAT64_SCHEMA)
      .put(double.class, () -> Schema.FLOAT64_SCHEMA)
      .build();

  private static Map<Pair<Schema.Type, Schema.Type>, Schema> ARITHMETIC_TYPE_MAPPINGS =
      ImmutableMap.<Pair<Schema.Type, Schema.Type>, Schema>builder()
          .put(new Pair<>(Schema.Type.INT64, Schema.Type.INT64), Schema.OPTIONAL_INT64_SCHEMA)
          .put(new Pair<>(Schema.Type.INT32, Schema.Type.INT64), Schema.OPTIONAL_INT64_SCHEMA)
          .put(new Pair<>(Schema.Type.INT64, Schema.Type.INT32), Schema.OPTIONAL_INT64_SCHEMA)
          .put(new Pair<>(Schema.Type.INT32, Schema.Type.INT32), Schema.OPTIONAL_INT32_SCHEMA)
          .put(new Pair<>(Schema.Type.FLOAT64, Schema.Type.FLOAT64), Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put(new Pair<>(Schema.Type.FLOAT64, Schema.Type.INT32), Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put(new Pair<>(Schema.Type.INT32, Schema.Type.FLOAT64), Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put(new Pair<>(Schema.Type.FLOAT64, Schema.Type.INT64), Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put(new Pair<>(Schema.Type.INT64, Schema.Type.FLOAT64), Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put(new Pair<>(Schema.Type.FLOAT32, Schema.Type.FLOAT64), Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put(new Pair<>(Schema.Type.FLOAT64, Schema.Type.FLOAT32), Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put(new Pair<>(Schema.Type.STRING, Schema.Type.STRING), Schema.OPTIONAL_STRING_SCHEMA)
          .build();

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
        return List.class;
      case MAP:
        return Map.class;
      case STRUCT:
        return Struct.class;
      default:
        throw new KsqlException("Type is not supported: " + schema.type());
    }
  }

  public static Schema getSchemaFromType(final Type type) {
    return typeToSchema.getOrDefault(type, () -> handleParametrizedType(type)).get();
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
        return Schema.OPTIONAL_STRING_SCHEMA;
      case "BOOLEAN":
      case "BOOL":
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
      case "INTEGER":
      case "INT":
        return Schema.OPTIONAL_INT32_SCHEMA;
      case "BIGINT":
      case "LONG":
        return Schema.OPTIONAL_INT64_SCHEMA;
      case "DOUBLE":
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      default:
        return getKsqlComplexType(sqlType);
    }
  }

  private static Schema getKsqlComplexType(final String sqlType) {
    if (sqlType.startsWith(ARRAY)) {
      return SchemaBuilder.array(
          getTypeSchema(
              sqlType.substring(
                  ARRAY.length() + 1,
                  sqlType.length() - 1
              )
          )
      ).optional().build();
    } else if (sqlType.startsWith(MAP)) {
      //TODO: For now only primitive data types for map are supported. Will have to add nested
      // types.
      String[] mapTypesStrs = sqlType
          .substring("MAP".length() + 1, sqlType.length() - 1)
          .trim()
          .split(",");
      if (mapTypesStrs.length != 2) {
        throw new KsqlException("Map type is not defined correctly.: " + sqlType);
      }
      String keyType = mapTypesStrs[0].trim();
      String valueType = mapTypesStrs[1].trim();
      return SchemaBuilder.map(getTypeSchema(keyType), getTypeSchema(valueType))
          .optional().build();
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

  private static final ImmutableMap<String, String> TYPE_MAP =
      new ImmutableMap.Builder<String, String>()
          .put("STRING", "VARCHAR(STRING)")
          .put("INT64", "BIGINT")
          .put("INT32", "INTEGER")
          .put("FLOAT64", "DOUBLE")
          .put("BOOLEAN", "BOOLEAN")
          .put("ARRAY", "ARRAY")
          .put("MAP", "MAP")
          .put("STRUCT", "STRUCT")
          .build();

  public static String getSchemaTypeAsSqlType(final Schema.Type type) {
    final String sqlType = TYPE_MAP.get(type.name());
    if (sqlType == null) {
      throw new IllegalArgumentException("Unknown schema type: " + type);
    }

    return sqlType;
  }

  public static String getJavaCastString(final Schema schema) {
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

  public static Schema addImplicitRowTimeRowKeyToSchema(final Schema schema) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA);
    schemaBuilder.field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA);
    for (Field field : schema.fields()) {
      if (!field.name().equals(SchemaUtil.ROWKEY_NAME)
          && !field.name().equals(SchemaUtil.ROWTIME_NAME)) {
        schemaBuilder.field(field.name(), field.schema());
      }
    }
    return schemaBuilder.build();
  }

  public static Schema removeImplicitRowTimeRowKeyFromSchema(final Schema schema) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (Field field : schema.fields()) {
      String fieldName = field.name();
      fieldName = fieldName.substring(fieldName.indexOf('.') + 1);
      if (!fieldName.equalsIgnoreCase(SchemaUtil.ROWTIME_NAME)
          && !fieldName.equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
        schemaBuilder.field(fieldName, field.schema());
      }
    }
    return schemaBuilder.build();
  }

  public static Set<Integer> getRowTimeRowKeyIndexes(final Schema schema) {
    Set<Integer> indexSet = new HashSet<>();
    for (int i = 0; i < schema.fields().size(); i++) {
      Field field = schema.fields().get(i);
      if (field.name().equalsIgnoreCase(SchemaUtil.ROWTIME_NAME)
          || field.name().equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
        indexSet.add(i);
      }
    }
    return indexSet;
  }

  public static String getSchemaDefinitionString(final Schema schema) {
    return schema.fields().stream()
        .map(field -> field.name() + " : " + getSqlTypeName(field.schema()))
        .collect(Collectors.joining(", ", "[", "]"));
  }

  public static String getSqlTypeName(final Schema schema) {
    switch (schema.type()) {
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "VARCHAR";
      case ARRAY:
        return "ARRAY<" + getSqlTypeName(schema.valueSchema()) + ">";
      case MAP:
        return "MAP<"
            + getSqlTypeName(schema.keySchema())
            + ","
            + getSqlTypeName(schema.valueSchema())
            + ">";
      case STRUCT:
        return getStructString(schema);
      default:
        throw new KsqlException(String.format("Invalid type in schema: %s.", schema.toString()));
    }
  }

  private static String getStructString(final Schema schema) {
    return schema.fields().stream()
        .map(field -> field.name() + " " + getSqlTypeName(field.schema()))
        .collect(Collectors.joining(", ", "STRUCT <", ">"));
  }

  public static String buildAvroSchema(final Schema schema, String name) {

    org.apache.avro.SchemaBuilder.FieldAssembler fieldAssembler = org.apache.avro.SchemaBuilder
        .record(name).namespace("ksql")
        .fields();
    for (Field field : schema.fields()) {
      fieldAssembler
          .name(field.name().replace(".", "_"))
          .type(getAvroSchemaForField(field.schema()))
          .withDefault(null);
    }

    return fieldAssembler.endRecord().toString();
  }

  private static org.apache.avro.Schema getAvroSchemaForField(final Schema fieldSchema) {
    switch (fieldSchema.type()) {
      case STRING:
        return unionWithNull(create(org.apache.avro.Schema.Type.STRING));
      case BOOLEAN:
        return unionWithNull(create(org.apache.avro.Schema.Type.BOOLEAN));
      case INT32:
        return unionWithNull(create(org.apache.avro.Schema.Type.INT));
      case INT64:
        return unionWithNull(create(org.apache.avro.Schema.Type.LONG));
      case FLOAT64:
        return unionWithNull(create(org.apache.avro.Schema.Type.DOUBLE));
      default:
        if (fieldSchema.type() == Schema.Type.ARRAY) {
          return unionWithNull(
              createArray(getAvroSchemaForField(fieldSchema.valueSchema())));
        } else if (fieldSchema.type() == Schema.Type.MAP) {
          return unionWithNull(
              createMap(getAvroSchemaForField(fieldSchema.valueSchema())));
        }
        throw new KsqlException("Unsupported AVRO type: " + fieldSchema.type().name());
    }
  }

  private static org.apache.avro.Schema unionWithNull(final org.apache.avro.Schema schema) {
    return createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), schema);
  }

  /**
   * Rename field names to be consistent with the internal column names.
   */
  public static Schema getAvroSerdeKsqlSchema(final Schema schema) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (Field field : schema.fields()) {
      schemaBuilder.field(field.name().replace(".", "_"), field.schema());
    }

    return schemaBuilder.build();
  }

  public static String getFieldNameWithNoAlias(final Field field) {
    String name = field.name();
    if (name.contains(".")) {
      return name.substring(name.indexOf(".") + 1);
    } else {
      return name;
    }
  }

  /**
   * Remove the alias when reading/writing from outside
   */
  public static Schema getSchemaWithNoAlias(final Schema schema) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (Field field : schema.fields()) {
      String name = getFieldNameWithNoAlias(field);
      schemaBuilder.field(name, field.schema());
    }
    return schemaBuilder.build();
  }

  public static boolean areEqualSchemas(final Schema schema1, final Schema schema2) {
    if (schema1.fields().size() != schema2.fields().size()) {
      return false;
    }
    for (int i = 0; i < schema1.fields().size(); i++) {
      if (!schema1.fields().get(i).equals(schema2.fields().get(i))) {
        return false;
      }
    }
    return true;
  }

  public static int getIndexInSchema(final String fieldName, final Schema schema) {
    List<Field> fields = schema.fields();
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      if (field.name().equals(fieldName)) {
        return i;
      }
    }
    throw new KsqlException(
        "Couldn't find field with name="
            + fieldName
            + " in schema. fields="
            + fields
    );
  }

  static Schema resolveArithmeticType(final Schema.Type left,
                                      final Schema.Type right) {

    final Schema schema = ARITHMETIC_TYPE_MAPPINGS.get(new Pair<>(left, right));
    if (schema == null) {
      throw new KsqlException("Unsupported arithmetic types. " + left + " " + right);
    }
    return schema;
  }


  private static Schema handleParametrizedType(final Type type) {
    if (type instanceof ParameterizedType) {
      final ParameterizedType parameterizedType = (ParameterizedType) type;
      if (parameterizedType.getRawType() == Map.class) {
        return SchemaBuilder.map(getSchemaFromType(
            parameterizedType.getActualTypeArguments()[0]),
            getSchemaFromType(parameterizedType.getActualTypeArguments()[1]));
      } else if (parameterizedType.getRawType() == List.class) {
        return SchemaBuilder.array(getSchemaFromType(
            parameterizedType.getActualTypeArguments()[0]));
      }
    }
    throw new KsqlException("Type is not supported: " + type);
  }
}
