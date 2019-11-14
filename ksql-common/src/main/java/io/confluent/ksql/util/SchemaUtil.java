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

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.BooleanType;
import io.confluent.ksql.function.types.DecimalType;
import io.confluent.ksql.function.types.DoubleType;
import io.confluent.ksql.function.types.IntegerType;
import io.confluent.ksql.function.types.LongType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.StringType;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
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

  public static boolean areCompatible(SqlType actual, ParamType declared) {
    if (actual.baseType() == SqlBaseType.ARRAY && declared instanceof ArrayType) {
      return areCompatible(((SqlArray) actual).getItemType(), ((ArrayType) declared).element());
    }

    if (actual.baseType() == SqlBaseType.MAP && declared instanceof MapType) {
      return areCompatible(
          ((SqlMap) actual).getValueType(),
          ((MapType) declared).value()
      );
    }

    if (actual.baseType() == SqlBaseType.STRUCT && declared instanceof StructType) {
      return isStructCompatible(actual, declared);
    }

    return isPrimitiveMatch(actual, declared);
  }

  private static boolean isStructCompatible(SqlType actual, ParamType declared) {
    final SqlStruct actualStruct = (SqlStruct) actual;

    // consider a struct that is empty to match any other struct
    if (actualStruct.fields().isEmpty() || ((StructType) declared).getSchema().isEmpty()) {
      return true;
    }

    for (Entry<String, ParamType> entry : ((StructType) declared).getSchema().entrySet()) {
      String k = entry.getKey();
      final Optional<io.confluent.ksql.schema.ksql.types.Field> field = actualStruct.field(k);
      if (!field.isPresent() || !areCompatible(field.get().type(), entry.getValue())) {
        return false;
      }
    }
    return actualStruct.fields().size() == ((StructType) declared).getSchema().size();
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  private static boolean isPrimitiveMatch(SqlType actual, ParamType declared) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    // CHECKSTYLE_RULES.OFF: BooleanExpressionComplexity
    return actual.baseType() == SqlBaseType.STRING  && declared instanceof StringType
        || actual.baseType() == SqlBaseType.INTEGER && declared instanceof IntegerType
        || actual.baseType() == SqlBaseType.BIGINT  && declared instanceof LongType
        || actual.baseType() == SqlBaseType.BOOLEAN && declared instanceof BooleanType
        || actual.baseType() == SqlBaseType.DOUBLE  && declared instanceof DoubleType
        || actual.baseType() == SqlBaseType.DECIMAL && declared instanceof DecimalType;
    // CHECKSTYLE_RULES.ON: BooleanExpressionComplexity
  }
}