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

package io.confluent.ksql.serde.util;

import io.confluent.ksql.util.KsqlException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public final class SerdeUtils {

  public static final String DESERIALIZER_LOGGER_NAME = "deserializer";

  private SerdeUtils() {
  }

  public static boolean toBoolean(final Object object) {
    Objects.requireNonNull(object, "Object cannot be null");
    if (object instanceof Boolean) {
      return (Boolean) object;
    }
    throw new IllegalArgumentException("This Object doesn't represent a boolean");
  }

  public static int toInteger(final Object object) {
    Objects.requireNonNull(object, "Object cannot be null");
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
        throw new KsqlException("Cannot convert " + object + " to INT.", e);
      }

    }
    throw new IllegalArgumentException("This Object doesn't represent an int");
  }

  public static long toLong(final Object object) {
    Objects.requireNonNull(object, "Object cannot be null");
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
        throw new KsqlException("Cannot convert " + object + " to BIGINT.", e);
      }

    }
    throw new IllegalArgumentException("This Object doesn't represent a long");
  }

  public static double toDouble(final Object object) {
    Objects.requireNonNull(object, "Object cannot be null");
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
        throw new KsqlException("Cannot convert " + object + " to DOUBLE.", e);
      }
    }
    throw new IllegalArgumentException("This Object doesn't represent a double");
  }

  public static boolean isCoercible(final Object object, final Schema targetSchema) {
    if (object == null) {
      return true;
    }

    switch (targetSchema.type()) {
      case BOOLEAN:
        return object instanceof Boolean;
      case INT32:
      case INT64:
      case FLOAT64:
        return isCoercibleToNumber(object);
      case STRING:
        return true;
      case ARRAY:
        return isCoercibleToArray(object, targetSchema.valueSchema());
      case MAP:
        return isCoercibleToMap(object, targetSchema.keySchema(), targetSchema.valueSchema());
      case STRUCT:
        return isCoercibleToStruct(object, targetSchema.fields());
      default:
        throw new UnsupportedOperationException("Unsupported type: " + targetSchema.type());
    }
  }

  private static boolean isCoercibleToNumber(final Object object) {
    return object instanceof Number || object instanceof String;
  }

  private static boolean isCoercibleToArray(final Object object, final Schema valueSchema) {
    if (!(object instanceof Collection)) {
      return false;
    }

    for (final Object element : ((Collection<?>) object)) {
      if (!isCoercible(element, valueSchema)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isCoercibleToMap(
      final Object object,
      final Schema keySchema,
      final Schema valueSchema) {
    if (!(object instanceof Map)) {
      return false;
    }

    for (final Map.Entry<?, ?> e : ((Map<?, ?>) object).entrySet()) {
      if (!isCoercible(e.getKey(), keySchema)) {
        return false;
      }

      if (!isCoercible(e.getValue(), valueSchema)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isCoercibleToStruct(final Object object, final List<Field> fields) {
    if (!(object instanceof Map)) {
      return false;
    }

    for (final Map.Entry<?, ?> e : ((Map<?, ?>) object).entrySet()) {
      final String fieldName = e.getKey().toString();
      final Field field = fields.stream()
          .filter(f -> f.name().equalsIgnoreCase(fieldName))
          .findFirst()
          .orElse(null);

      if (field == null) {
        return false;
      }

      if (!isCoercible(e.getValue(), field.schema())) {
        return false;
      }
    }
    return true;
  }
}
