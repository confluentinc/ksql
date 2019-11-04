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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.function.GenericsUtil;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class SchemaUtil {

  public static final ColumnName ROWKEY_NAME = ColumnName.of("ROWKEY");
  public static final ColumnName ROWTIME_NAME = ColumnName.of("ROWTIME");
  public static final ColumnName WINDOWSTART_NAME = ColumnName.of("WINDOWSTART");

  public static final int ROWKEY_INDEX = 1;

  private static final Set<Schema.Type> ARITHMETIC_TYPES = ImmutableSet.of(
      Type.INT8,
      Type.INT16,
      Type.INT32,
      Type.INT64,
      Type.FLOAT32,
      Type.FLOAT64
  );

  private static final char FIELD_NAME_DELIMITER = '.';

  private static final Map<Type, BiPredicate<Schema, Schema>> CUSTOM_SCHEMA_EQ =
      ImmutableMap.<Type, BiPredicate<Schema, Schema>>builder()
          .put(Type.MAP, SchemaUtil::mapCompatible)
          .put(Type.ARRAY, SchemaUtil::arrayCompatible)
          .put(Type.STRUCT, SchemaUtil::structCompatible)
          .put(Type.BYTES, SchemaUtil::bytesEquals)
          .build();


  private SchemaUtil() {
  }

  // Do Not use in new code - use `SchemaConverters` directly.
  public static Class<?> getJavaType(final Schema schema) {
    return SchemaConverters.sqlToJavaConverter().toJavaType(
        SchemaConverters.connectToSqlConverter().toSqlType(schema)
    );
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

  public static String buildAliasedFieldName(final String alias, final String fieldName) {
    final String prefix = alias + FIELD_NAME_DELIMITER;
    if (fieldName.startsWith(prefix)) {
      return fieldName;
    }
    return prefix + fieldName;
  }

  public static String getFieldNameWithNoAlias(final String fieldName) {
    final int idx = fieldName.indexOf(FIELD_NAME_DELIMITER);
    if (idx < 0) {
      return fieldName;
    }

    return fieldName.substring(idx + 1);
  }

  public static boolean isNumber(final Schema schema) {
    return ARITHMETIC_TYPES.contains(schema.type()) || DecimalUtil.isDecimal(schema);
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


  public static boolean areCompatible(final Schema arg1, final Schema arg2) {
    if (arg2 == null) {
      return arg1.isOptional();
    }

    // we require a custom equals method that ignores certain values (e.g.
    // whether or not the schema is optional, and the documentation)
    return Objects.equals(arg1.type(), arg2.type())
        && CUSTOM_SCHEMA_EQ.getOrDefault(arg1.type(), (a, b) -> true).test(arg1, arg2)
        && Objects.equals(arg1.version(), arg2.version())
        && Objects.deepEquals(arg1.defaultValue(), arg2.defaultValue());
  }

  private static boolean mapCompatible(final Schema mapA, final Schema mapB) {
    return areCompatible(mapA.keySchema(), mapB.keySchema())
        && areCompatible(mapA.valueSchema(), mapB.valueSchema());
  }

  private static boolean arrayCompatible(final Schema arrayA, final Schema arrayB) {
    return areCompatible(arrayA.valueSchema(), arrayB.valueSchema());
  }

  private static boolean structCompatible(final Schema structA, final Schema structB) {
    return structA.fields().isEmpty()
        || structB.fields().isEmpty()
        || compareFieldsOfStructs(structA, structB);
  }


  private static boolean compareFieldsOfStructs(final Schema structA, final Schema structB) {

    final List<Field> fieldsA = structA.fields();
    final List<Field> fieldsB = structB.fields();
    final int sizeA = fieldsA.size();
    final int sizeB = fieldsB.size();

    if (sizeA != sizeB) {
      return false;
    }

    // Custom field comparison to support comparison of structs with decimals and generics
    for (int i = 0; i < sizeA; i++) {
      final Field fieldA = fieldsA.get(i);
      final Field fieldB = fieldsB.get(i);
      if (!fieldA.name().equals(fieldB.name())
          || fieldA.index() != fieldB.index()
          || ! areCompatible(fieldsA.get(i).schema(), fieldsB.get(i).schema())) {
        return false;
      }
    }
    return true;
  }

  private static boolean bytesEquals(final Schema bytesA, final Schema bytesB) {
    // two datatypes are currently represented as bytes: generics and decimals

    if (GenericsUtil.isGeneric(bytesA)) {
      if (GenericsUtil.isGeneric(bytesB)) {
        return bytesA.name().equals(bytesB.name());
      }
      return false;
    }

    // from a Java schema perspective, all decimals are the same
    // since they can all be cast to BigDecimal
    return DecimalUtil.isDecimal(bytesA) && DecimalUtil.isDecimal(bytesB);
  }

}