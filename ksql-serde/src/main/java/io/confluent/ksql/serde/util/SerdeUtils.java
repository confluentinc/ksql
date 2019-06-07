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

package io.confluent.ksql.serde.util;

import io.confluent.ksql.util.KsqlException;
import java.util.Objects;

public class SerdeUtils {
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
}
