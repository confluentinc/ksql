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

package io.confluent.ksql.util;

import static org.apache.avro.Schema.create;
import static org.apache.avro.Schema.createArray;
import static org.apache.avro.Schema.createMap;
import static org.apache.avro.Schema.createUnion;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public final class SchemaUtil {

  private static final String DEFAULT_NAMESPACE = "ksql";

  public static final String ARRAY = "ARRAY";
  public static final String MAP = "MAP";
  public static final String STRUCT = "STRUCT";

  public static final String ROWKEY_NAME = "ROWKEY";
  public static final String ROWTIME_NAME = "ROWTIME";

  public static final int ROWTIME_INDEX = 0;
  public static final int ROWKEY_INDEX = 1;
  private static final Map<Type, Supplier<SchemaBuilder>> typeToSchema
      = ImmutableMap.<Type, Supplier<SchemaBuilder>>builder()
      .put(String.class, () -> SchemaBuilder.string().optional())
      .put(boolean.class, SchemaBuilder::bool)
      .put(Boolean.class, () -> SchemaBuilder.bool().optional())
      .put(Integer.class, () -> SchemaBuilder.int32().optional())
      .put(int.class, SchemaBuilder::int32)
      .put(Long.class, () -> SchemaBuilder.int64().optional())
      .put(long.class, SchemaBuilder::int64)
      .put(Double.class, () -> SchemaBuilder.float64().optional())
      .put(double.class, SchemaBuilder::float64)
      .build();

  @VisibleForTesting
  static final List<Schema.Type> ARITHMETIC_TYPES_LIST =
      ImmutableList.of(
          Schema.Type.INT8,
          Schema.Type.INT16,
          Schema.Type.INT32,
          Schema.Type.INT64,
          Schema.Type.FLOAT32,
          Schema.Type.FLOAT64
      );

  private static final Set<Schema.Type> ARITHMETIC_TYPES =
      ImmutableSet.copyOf(ARITHMETIC_TYPES_LIST);

  private static final Ordering<Schema.Type> ARITHMETIC_TYPE_ORDERING = Ordering.explicit(
      ARITHMETIC_TYPES_LIST
  );

  private static final NavigableMap<Schema.Type, Schema> TYPE_TO_SCHEMA =
      ImmutableSortedMap.<Schema.Type, Schema>orderedBy(ARITHMETIC_TYPE_ORDERING)
          .put(Schema.Type.INT32, Schema.OPTIONAL_INT32_SCHEMA)
          .put(Schema.Type.INT64, Schema.OPTIONAL_INT64_SCHEMA)
          .put(Schema.Type.FLOAT32, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put(Schema.Type.FLOAT64, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .build();

  private static final ImmutableMap<String, String> SCHEMA_TYPE_NAME_TO_SQL_TYPE =
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

  private static final Map<Schema.Type, Class<?>> SCHEMA_TYPE_TO_JAVA_TYPE =
      ImmutableMap.<Schema.Type, Class<?>>builder()
          .put(Schema.Type.STRING, String.class)
          .put(Schema.Type.BOOLEAN, Boolean.class)
          .put(Schema.Type.INT32, Integer.class)
          .put(Schema.Type.INT64, Long.class)
          .put(Schema.Type.FLOAT64, Double.class)
          .put(Schema.Type.ARRAY, List.class)
          .put(Schema.Type.MAP, Map.class)
          .put(Schema.Type.STRUCT, Struct.class)
          .build();

  public static final char FIELD_NAME_DELIMITER = '.';

  private static final Map<Schema.Type, Function<Schema, String>> SCHEMA_TYPE_TO_SQL_TYPE =
      ImmutableMap.<Schema.Type, Function<Schema, String>>builder()
          .put(Schema.Type.INT32, s -> "INT")
          .put(Schema.Type.INT64, s -> "BIGINT")
          .put(Schema.Type.FLOAT32, s -> "DOUBLE")
          .put(Schema.Type.FLOAT64, s -> "DOUBLE")
          .put(Schema.Type.BOOLEAN, s -> "BOOLEAN")
          .put(Schema.Type.STRING, s -> "VARCHAR")
          .put(Schema.Type.ARRAY, s ->
              "ARRAY<" + getSqlTypeName(s.valueSchema()) + ">")
          .put(Schema.Type.MAP, s ->
              "MAP<" + getSqlTypeName(s.keySchema()) + "," + getSqlTypeName(s.valueSchema()) + ">")
          .put(Schema.Type.STRUCT, s -> getStructString(s))
          .build();

  private static final ImmutableMap<Schema.Type, String> SCHEMA_TYPE_TO_CAST_STRING =
      new ImmutableMap.Builder<Schema.Type, String>()
          .put(Schema.Type.INT32, "(Integer)")
          .put(Schema.Type.INT64, "(Long)")
          .put(Schema.Type.FLOAT64, "(Double)")
          .put(Schema.Type.STRING, "(String)")
          .put(Schema.Type.BOOLEAN, "(Boolean)")
          .build();


  private SchemaUtil() {
  }

  public static Class<?> getJavaType(final Schema schema) {
    final Class<?> typeClazz = SCHEMA_TYPE_TO_JAVA_TYPE.get(schema.type());
    if (typeClazz == null) {
      throw new KsqlException("Type is not supported: " + schema.type());
    }

    return typeClazz;
  }

  public static Schema getSchemaFromType(final Type type) {
    return getSchemaFromType(type, null, null);
  }

  public static Schema getSchemaFromType(final Type type, final String name, final String doc) {
    final SchemaBuilder schema =
        typeToSchema.getOrDefault(type, () -> handleParametrizedType(type)).get();

    schema.name(name);
    schema.doc(doc);
    return schema.build();
  }

  public static boolean matchFieldName(final Field field, final String fieldName) {
    return field.name().equals(fieldName)
        || field.name().equals(getFieldNameWithNoAlias(fieldName));
  }

  /**
   * Check if the supplied {@code actual} field name matches the supplied {@code required}.
   *
   * <p>Note: if {@code required} is not aliases and {@code actual} is, then the alias is stripped
   * from {@code actual} to allow a match.
   * @param actual   the field name to be checked
   * @param required the required field name.
   * @return {@code true} on a match, {@code false} otherwise.
   */
  public static boolean isFieldName(final String actual, final String required) {
    return required.equals(actual)
        || required.equals(getFieldNameWithNoAlias(actual));
  }

  public static Field buildAliasedField(final String alias, final Field field) {
    return new Field(buildAliasedFieldName(alias, field.name()), field.index(), field.schema());
  }

  public static String buildAliasedFieldName(final String alias, final String fieldName) {
    final String prefix = alias + FIELD_NAME_DELIMITER;
    if (fieldName.startsWith(prefix)) {
      return fieldName;
    }
    return prefix + fieldName;
  }

  public static String getSchemaTypeAsSqlType(final Schema.Type type) {
    final String sqlType = SCHEMA_TYPE_NAME_TO_SQL_TYPE.get(type.name());
    if (sqlType == null) {
      throw new IllegalArgumentException("Unknown schema type: " + type);
    }

    return sqlType;
  }

  public static String getJavaCastString(final Schema schema) {
    final String castString = SCHEMA_TYPE_TO_CAST_STRING.get(schema.type());
    if (castString == null) {
      //TODO: Add complex or other types later!
      return "";
    }

    return castString;
  }

  public static String getSqlTypeName(final Schema schema) {
    final Function<Schema, String> handler = SCHEMA_TYPE_TO_SQL_TYPE.get(schema.type());
    if (handler == null) {
      throw new KsqlException(String.format("Invalid type in schema: %s.", schema.toString()));
    }

    return handler.apply(schema);
  }

  private static String getStructString(final Schema schema) {
    return schema.fields().stream()
        .map(field -> field.name() + " " + getSqlTypeName(field.schema()))
        .collect(Collectors.joining(", ", "STRUCT<", ">"));
  }

  public static org.apache.avro.Schema buildAvroSchema(final Schema schema, final String name) {
    return buildAvroSchema(DEFAULT_NAMESPACE, name, schema);
  }

  private static org.apache.avro.Schema buildAvroSchema(
      final String namespace,
      final String name,
      final Schema schema
  ) {
    final String avroName = avroify(name);
    final FieldAssembler<org.apache.avro.Schema> fieldAssembler = org.apache.avro.SchemaBuilder
        .record(avroName).namespace(namespace)
        .fields();

    for (final Field field : schema.fields()) {
      final String fieldName = avroify(field.name());
      final String fieldNamespace = namespace + "." + avroName;

      fieldAssembler
          .name(fieldName)
          .type(getAvroSchemaForField(fieldNamespace, fieldName, field.schema()))
          .withDefault(null);
    }

    return fieldAssembler.endRecord();
  }

  private static String avroify(final String name) {
    return name
        .replace(".", "_")
        .replace("-", "_");
  }

  private static org.apache.avro.Schema getAvroSchemaForField(
      final String namespace,
      final String fieldName,
      final Schema fieldSchema
  ) {
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
      case ARRAY:
        return unionWithNull(createArray(
            getAvroSchemaForField(namespace, fieldName, fieldSchema.valueSchema())));
      case MAP:
        return unionWithNull(createMap(
            getAvroSchemaForField(namespace, fieldName, fieldSchema.valueSchema())));
      case STRUCT:
        return unionWithNull(buildAvroSchema(namespace, fieldName, fieldSchema));
      default:
        throw new KsqlException("Unsupported AVRO type: " + fieldSchema.type().name());
    }
  }

  private static org.apache.avro.Schema unionWithNull(final org.apache.avro.Schema schema) {
    return createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), schema);
  }

  public static String getFieldNameWithNoAlias(final Field field) {
    final String name = field.name();
    return getFieldNameWithNoAlias(name);
  }

  public static String getFieldNameWithNoAlias(final String fieldName) {
    final int idx = fieldName.indexOf(FIELD_NAME_DELIMITER);
    if (idx < 0) {
      return fieldName;
    }

    return fieldName.substring(idx + 1);
  }

  public static Schema resolveBinaryOperatorResultType(final Schema.Type left,
                                                       final Schema.Type right) {
    if (left == Schema.Type.STRING && right == Schema.Type.STRING) {
      return Schema.OPTIONAL_STRING_SCHEMA;
    }

    if (!TYPE_TO_SCHEMA.containsKey(left) || !TYPE_TO_SCHEMA.containsKey(right)) {
      throw new KsqlException("Unsupported arithmetic types. " + left + " " + right);
    }

    return TYPE_TO_SCHEMA.ceilingEntry(ARITHMETIC_TYPE_ORDERING.max(left, right)).getValue();
  }

  public static boolean isNumber(final Schema.Type type) {
    return ARITHMETIC_TYPES.contains(type);
  }

  private static SchemaBuilder handleParametrizedType(final Type type) {
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
    } else if (type instanceof Class<?> && ((Class<?>) type).isArray()) {
      // handle var args
      return SchemaBuilder.array(getSchemaFromType(((Class<?>) type).getComponentType()));
    }
    throw new KsqlException("Type inference is not supported for: " + type);
  }
}
