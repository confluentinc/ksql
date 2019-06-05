/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.serde.json;

import io.confluent.ksql.schema.SqlType;
import io.confluent.ksql.schema.connect.SchemaWalker;
import io.confluent.ksql.schema.connect.SchemaWalker.Visitor;
import io.confluent.ksql.schema.persistence.PersistenceSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

final class JsonSerdeUtils {

  private JsonSerdeUtils() {
  }

  static PersistenceSchema validateSchema(final PersistenceSchema schema) {

    class SchemaValidator implements Visitor {

      @Override
      public boolean visitMap(final Schema schema) {
        if (schema.keySchema().type() != Type.STRING) {
          throw new IllegalArgumentException("Only MAPs with STRING keys are supported");
        }
        return true;
      }
    }

    SchemaWalker.visit(schema.getConnectSchema(), new SchemaValidator());
    return schema;
  }

  static boolean toBoolean(final Object object) {
    if (object instanceof Boolean) {
      return (Boolean) object;
    }
    throw invalidConversionException(object, SqlType.BOOLEAN);
  }

  static int toInteger(final Object object) {
    if (object instanceof Integer) {
      return (Integer) object;
    }
    if (object instanceof Number) {
      return ((Number) object).intValue();
    }
    if (object instanceof String) {
      try {
        return Integer.parseInt((String) object);
      } catch (final NumberFormatException e) {
        throw failedStringCoercionException(SqlType.INTEGER);
      }
    }
    throw invalidConversionException(object, SqlType.INTEGER);
  }

  static long toLong(final Object object) {
    if (object instanceof Long) {
      return (Long) object;
    }
    if (object instanceof Number) {
      return ((Number) object).longValue();
    }
    if (object instanceof String) {
      try {
        return Long.parseLong((String) object);
      } catch (final NumberFormatException e) {
        throw failedStringCoercionException(SqlType.BIGINT);
      }
    }
    throw invalidConversionException(object, SqlType.BIGINT);
  }

  static double toDouble(final Object object) {
    if (object instanceof Double) {
      return (Double) object;
    }
    if (object instanceof Number) {
      return ((Number) object).doubleValue();
    }
    if (object instanceof String) {
      try {
        return Double.parseDouble((String) object);
      } catch (final NumberFormatException e) {
        throw failedStringCoercionException(SqlType.DOUBLE);
      }
    }
    throw invalidConversionException(object, SqlType.DOUBLE);
  }

  static IllegalArgumentException invalidConversionException(
      final Object object,
      final String sqlType
  ) {
    return new IllegalArgumentException("Can't convert type."
        + " sourceType: " + object.getClass().getSimpleName()
        + ", requiredType: " + sqlType);
  }

  private static IllegalArgumentException invalidConversionException(
      final Object object,
      final SqlType sqlType
  ) {
    return invalidConversionException(object, sqlType.toString());
  }

  private static IllegalArgumentException failedStringCoercionException(final SqlType sqlType) {
    return new IllegalArgumentException("Can't coerce string to type. targetType: " + sqlType);
  }
}
