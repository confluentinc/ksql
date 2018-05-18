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

public class SerdeUtils {

  public static boolean toBoolean(Object object) {
    if (object instanceof Boolean) {
      return (Boolean) object;
    }
    throw new IllegalArgumentException("This Object doesn't represent a boolean");
  }

  public static int toInteger(Object object) {
    if (object instanceof Integer) {
      return (Integer) object;
    } else if (object instanceof Number) {
      return ((Number) object).intValue();
    } else if (object instanceof String) {
      return Integer.parseInt((String) object);
    }
    throw new IllegalArgumentException("This Object doesn't represent an int");
  }

  public static long toLong(Object object) {
    if (object instanceof Long) {
      return (Long) object;
    } else if (object instanceof Number) {
      return ((Number) object).longValue();
    } else if (object instanceof String) {
      return Long.parseLong((String) object);
    }
    throw new IllegalArgumentException("This Object doesn't represent a long");
  }

  public static double toDouble(Object object) {
    if (object instanceof Double) {
      return (Double) object;
    } else if (object instanceof Number) {
      return ((Number) object).doubleValue();
    } else if (object instanceof String) {
      return Double.parseDouble((String) object);
    }
    throw new IllegalArgumentException("This Object doesn't represent a double");
  }

}