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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public final class SchemaUtil {

  private static final String DEFAULT_NAMESPACE = "ksql";

  public static final String ROWKEY_NAME = "ROWKEY";
  public static final String ROWTIME_NAME = "ROWTIME";

  public static final int ROWKEY_INDEX = 1;

  private static final List<Schema.Type> ARITHMETIC_TYPES_LIST =
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

  private static final char FIELD_NAME_DELIMITER = '.';

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
    if (DecimalUtil.isDecimal(schema)) {
      return BigDecimal.class;
    }

    final Class<?> typeClazz = SCHEMA_TYPE_TO_JAVA_TYPE.get(schema.type());
    if (typeClazz == null) {
      throw new KsqlException("Type is not supported: " + schema.type());
    }

    return typeClazz;
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

  public static String getJavaCastString(final Schema schema) {
    final String castString = SCHEMA_TYPE_TO_CAST_STRING.get(schema.type());
    if (castString == null) {
      return "";
    }

    return castString;
  }

  public static org.apache.avro.Schema buildAvroSchema(
      final PersistenceSchema schema,
      final String name
  ) {
    return buildAvroSchema(DEFAULT_NAMESPACE, name, schema.serializedSchema());
  }

  private static org.apache.avro.Schema buildAvroSchema(
      final String namespace,
      final String name,
      final Schema schema
  ) {
    switch (schema.type()) {
      case STRING:
        return create(org.apache.avro.Schema.Type.STRING);
      case BOOLEAN:
        return create(org.apache.avro.Schema.Type.BOOLEAN);
      case INT32:
        return create(org.apache.avro.Schema.Type.INT);
      case INT64:
        return create(org.apache.avro.Schema.Type.LONG);
      case FLOAT64:
        return create(org.apache.avro.Schema.Type.DOUBLE);
      case BYTES:
        return createBytesSchema(schema);
      case ARRAY:
        return createArray(unionWithNull(buildAvroSchema(namespace, name, schema.valueSchema())));
      case MAP:
        return createMap(unionWithNull(buildAvroSchema(namespace, name, schema.valueSchema())));
      case STRUCT:
        return buildAvroSchemaFromStruct(namespace, name, schema);
      default:
        throw new KsqlException("Unsupported AVRO type: " + schema.type().name());
    }
  }

  private static org.apache.avro.Schema createBytesSchema(
      final Schema schema
  ) {
    DecimalUtil.requireDecimal(schema);
    return LogicalTypes.decimal(DecimalUtil.precision(schema), DecimalUtil.scale(schema))
        .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));
  }

  private static org.apache.avro.Schema buildAvroSchemaFromStruct(
      final String namespace,
      final String name,
      final Schema schema
  ) {
    final String avroName = avroify(name);
    final FieldAssembler<org.apache.avro.Schema> fieldAssembler = org.apache.avro.SchemaBuilder
        .record(avroName)
        .namespace(namespace)
        .fields();

    for (final Field field : schema.fields()) {
      final String fieldName = avroify(field.name());
      final String fieldNamespace = namespace + "." + avroName;

      fieldAssembler
          .name(fieldName)
          .type(unionWithNull(buildAvroSchema(fieldNamespace, fieldName, field.schema())))
          .withDefault(null);
    }

    return fieldAssembler.endRecord();
  }

  private static String avroify(final String name) {
    return name
        .replace(".", "_")
        .replace("-", "_");
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

  public static Optional<String> getFieldNameAlias(final String fieldName) {
    final int idx = fieldName.indexOf(FIELD_NAME_DELIMITER);
    if (idx < 0) {
      return Optional.empty();
    }

    return Optional.of(fieldName.substring(0, idx));
  }

  public static Schema resolveBinaryOperatorResultType(
      final Schema left,
      final Schema right,
      final Operator operator
  ) {
    if (left.type() == Schema.Type.STRING && right.type() == Schema.Type.STRING) {
      return Schema.OPTIONAL_STRING_SCHEMA;
    }

    if (DecimalUtil.isDecimal(left) || DecimalUtil.isDecimal(right)) {
      if (left.type() != Schema.Type.FLOAT64 && right.type() != Schema.Type.FLOAT64) {
        return resolveDecimalOperatorResultType(
            DecimalUtil.toDecimal(left), DecimalUtil.toDecimal(right), operator);
      }
      return Schema.OPTIONAL_FLOAT64_SCHEMA;
    }

    if (!TYPE_TO_SCHEMA.containsKey(left.type()) || !TYPE_TO_SCHEMA.containsKey(right.type())) {
      throw new KsqlException("Unsupported arithmetic types. " + left.type() + " " + right.type());
    }

    return TYPE_TO_SCHEMA.ceilingEntry(
        ARITHMETIC_TYPE_ORDERING.max(left.type(), right.type())).getValue();
  }

  private static Schema resolveDecimalOperatorResultType(
      final Schema left,
      final Schema right,
      final Operator operator
  ) {
    final int lPrecision = DecimalUtil.precision(left);
    final int rPrecision = DecimalUtil.precision(right);
    final int lScale = DecimalUtil.scale(left);
    final int rScale = DecimalUtil.scale(right);

    final int precision;
    final int scale;
    switch (operator) {
      case ADD:
      case SUBTRACT:
        precision = Math.max(lScale, rScale)
            + Math.max(lPrecision - lScale, rPrecision - rScale)
            + 1;
        scale = Math.max(lScale, rScale);
        break;
      case MULTIPLY:
        precision = lPrecision + rPrecision + 1;
        scale = lScale + rScale;
        break;
      case DIVIDE:
        precision = lPrecision - lScale + rScale + Math.max(6, lScale + rPrecision + 1);
        scale = Math.max(6, lScale + rPrecision + 1);
        break;
      case MODULUS:
        precision = Math.min(lPrecision - lScale, rPrecision - rScale) + Math.max(lScale, rScale);
        scale = Math.max(lScale, rScale);
        break;
      default:
        throw new KsqlException("Unexpected operator type: " + operator);
    }

    return DecimalUtil.builder(precision, scale).build();
  }

  public static boolean isNumber(final Schema.Type type) {
    return ARITHMETIC_TYPES.contains(type);
  }

  public static boolean isNumber(final Schema schema) {
    return isNumber(schema.type()) || DecimalUtil.isDecimal(schema);
  }

  public static Schema ensureOptional(final Schema schema) {
    final SchemaBuilder builder;
    switch (schema.type()) {
      case STRUCT:
        builder = SchemaBuilder.struct();
        schema.fields()
            .forEach(f -> builder.field(f.name(), ensureOptional(f.schema())));
        break;

      case MAP:
        builder = SchemaBuilder.map(
            ensureOptional(schema.keySchema()),
            ensureOptional(schema.valueSchema())
        );
        break;

      case ARRAY:
        builder = SchemaBuilder.array(
            ensureOptional(schema.valueSchema())
        );
        break;

      default:
        if (schema.isOptional()) {
          return schema;
        }

        builder = new SchemaBuilder(schema.type());
        break;
    }

    return builder
        .name(schema.name())
        .optional()
        .build();
  }

}