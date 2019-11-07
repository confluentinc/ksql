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

package io.confluent.ksql.execution.util;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class GenericRowValueTypeEnforcer {

  private final List<Column> columns;

  private static final Map<SqlBaseType, Function<Object, Object>> SCHEMA_TYPE_TO_ENFORCE =
      ImmutableMap.<SqlBaseType, Function<Object, Object>>builder()
          .put(SqlBaseType.INTEGER, GenericRowValueTypeEnforcer::enforceInteger)
          .put(SqlBaseType.BIGINT, GenericRowValueTypeEnforcer::enforceLong)
          .put(SqlBaseType.DOUBLE, GenericRowValueTypeEnforcer::enforceDouble)
          .put(SqlBaseType.STRING, GenericRowValueTypeEnforcer::enforceString)
          .put(SqlBaseType.BOOLEAN, GenericRowValueTypeEnforcer::enforceBoolean)
          .put(SqlBaseType.DECIMAL, v -> v)
          .put(SqlBaseType.ARRAY, v -> v)
          .put(SqlBaseType.MAP, v -> v)
          .put(SqlBaseType.STRUCT, v -> v)
          .build();

  public GenericRowValueTypeEnforcer(LogicalSchema schema) {
    this.columns = schema.value();
  }

  public Object enforceColumnType(int index, Object value) {
    Column column = columns.get(index);
    return enforceColumnType(column.type(), value);
  }

  private static Object enforceColumnType(SqlType sqlType, Object value) {
    Function<Object, Object> handler = SCHEMA_TYPE_TO_ENFORCE.get(sqlType.baseType());
    if (handler == null) {
      throw new KsqlException("Type is not supported: " + sqlType);
    }

    return handler.apply(value);
  }

  private static Double enforceDouble(Object value) {
    if (value instanceof Double) {
      return (Double) value;
    } else if (value instanceof Integer) {
      return ((Integer) value).doubleValue();
    } else if (value instanceof Long) {
      return ((Long) value).doubleValue();
    } else if (value instanceof Float) {
      return ((Float) value).doubleValue();
    } else if (value instanceof Short) {
      return ((Short) value).doubleValue();
    } else if (value instanceof Byte) {
      return ((Byte) value).doubleValue();
    } else if (value instanceof CharSequence) {
      return Double.parseDouble(value.toString());
    } else if (value == null) {
      return null;
    } else {
      throw new KsqlException("Invalid field type. Value must be Double.");
    }
  }

  private static Long enforceLong(Object value) {
    if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof Integer) {
      return ((Integer) value).longValue();
    } else if (value instanceof Float) {
      return ((Float) value).longValue();
    } else if (value instanceof Short) {
      return ((Short) value).longValue();
    } else if (value instanceof Byte) {
      return ((Byte) value).longValue();
    } else if (value instanceof CharSequence) {
      return Long.parseLong(value.toString());
    } else if (value == null) {
      return null;
    } else {
      throw new KsqlException("Invalid field type. Value must be Long.");
    }
  }

  private static Integer enforceInteger(Object value) {

    if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Long) {
      return ((Long) value).intValue();
    } else if (value instanceof Float) {
      return ((Float) value).intValue();
    } else if (value instanceof Short) {
      return ((Short) value).intValue();
    } else if (value instanceof Byte) {
      return ((Byte) value).intValue();
    } else if (value instanceof CharSequence) {
      return Integer.parseInt(value.toString());
    } else if (value == null) {
      return null;
    } else {
      throw new KsqlException("Invalid field type. Value must be Integer.");
    }
  }

  private static String enforceString(Object value) {
    if (value instanceof CharSequence) {
      return value.toString();
    } else if (value == null) {
      return null;
    } else {
      throw new KsqlException("Invalid field type. Value must be String.");
    }
  }

  @SuppressFBWarnings("NP_BOOLEAN_RETURN_NULL")
  private static Boolean enforceBoolean(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean(value.toString());
    } else if (value == null) {
      return null;
    } else {
      throw new KsqlException("Invalid field type. Value must be Boolean.");
    }
  }
}