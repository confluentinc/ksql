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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.ksql.schema.connect.SchemaWalker;
import io.confluent.ksql.schema.connect.SchemaWalker.Visitor;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

final class JsonSerdeUtils {

  private JsonSerdeUtils() {
  }

  static PersistenceSchema validateSchema(final PersistenceSchema schema) {

    class SchemaValidator implements Visitor<Void, Void> {

      @Override
      public Void visitMap(final Schema schema, final Void key, final Void value) {
        if (schema.keySchema().type() != Type.STRING) {
          throw new IllegalArgumentException("Only MAPs with STRING keys are supported");
        }
        return null;
      }

      public Void visitSchema(final Schema schema) {
        return null;
      }
    }

    SchemaWalker.visit(schema.serializedSchema(), new SchemaValidator());
    return schema;
  }

  static boolean toBoolean(final JsonNode object) {
    if (object instanceof BooleanNode) {
      return object.booleanValue();
    }

    throw invalidConversionException(object, SqlBaseType.BOOLEAN);
  }

  static int toInteger(final JsonNode object) {
    if (object instanceof NumericNode) {
      return object.intValue();
    }
    if (object instanceof TextNode) {
      try {
        return Integer.parseInt(object.textValue());
      } catch (final NumberFormatException e) {
        throw failedStringCoercionException(SqlBaseType.INTEGER);
      }
    }
    throw invalidConversionException(object, SqlBaseType.INTEGER);
  }

  static long toLong(final JsonNode object) {
    if (object instanceof NumericNode) {
      return object.asLong();
    }
    if (object instanceof TextNode) {
      try {
        return Long.parseLong(object.textValue());
      } catch (final NumberFormatException e) {
        throw failedStringCoercionException(SqlBaseType.BIGINT);
      }
    }
    throw invalidConversionException(object, SqlBaseType.BIGINT);
  }

  static double toDouble(final JsonNode object) {
    if (object instanceof NumericNode) {
      return object.doubleValue();
    }
    if (object instanceof TextNode) {
      try {
        return Double.parseDouble(object.textValue());
      } catch (final NumberFormatException e) {
        throw failedStringCoercionException(SqlBaseType.DOUBLE);
      }
    }

    throw invalidConversionException(object, SqlBaseType.DOUBLE);
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
      final SqlBaseType sqlType
  ) {
    return invalidConversionException(object, sqlType.toString());
  }

  private static IllegalArgumentException failedStringCoercionException(final SqlBaseType sqlType) {
    return new IllegalArgumentException("Can't coerce string to type. targetType: " + sqlType);
  }
}
