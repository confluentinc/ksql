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

package io.confluent.ksql.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

public class GenericRowValueTypeEnforcer {

  private final List<Field> fields;

  public GenericRowValueTypeEnforcer(final Schema schema) {
    this.fields = schema.fields();
  }

  public Object enforceFieldType(final int index, final Object value) {
    Field field = fields.get(index);
    return enforceFieldType(field.schema(), value);
  }

  private Object enforceFieldType(Schema schema, final Object value) {

    switch (schema.type()) {
      case INT32:
        return enforceInteger(value);
      case INT64:
        return enforceLong(value);
      case FLOAT64:
        return enforceDouble(value);
      case STRING:
        return enforceString(value);
      case BOOLEAN:
        return enforceBoolean(value);
      case ARRAY:
        return value;
      case MAP:
        return value;
      default:
        throw new KsqlException("Type is not supported: " + schema);
    }
  }

  private Double enforceDouble(final Object value) {
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
    } else if (value instanceof String || value instanceof CharSequence) {
      return Double.parseDouble(value.toString());
    } else if (value == null) {
      return null;
    } else {
      throw new KsqlException("Invalid field type. Value must be Double.");
    }
  }

  private Long enforceLong(final Object value) {
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
    } else if (value instanceof String || value instanceof CharSequence) {
      return Long.parseLong(value.toString());
    } else if (value == null) {
      return null;
    } else {
      throw new KsqlException("Invalid field type. Value must be Long.");
    }
  }

  private Integer enforceInteger(final Object value) {

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
    } else if (value instanceof String || value instanceof CharSequence) {
      return Integer.parseInt(value.toString());
    } else if (value == null) {
      return null;
    } else {
      throw new KsqlException("Invalid field type. Value must be Integer.");
    }
  }

  private String enforceString(final Object value) {
    if (value instanceof String || value instanceof CharSequence) {
      return value.toString();
    } else if (value == null) {
      return null;
    } else {
      throw new KsqlException("Invalid field type. Value must be String.");
    }
  }

  private Boolean enforceBoolean(final Object value) {
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
