/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

public class GenericRowValueTypeEnforcer {

  Schema schema;
  List<Field> fields;

  public GenericRowValueTypeEnforcer(final Schema schema) {
    this.schema = schema;
    this.fields = schema.fields();
  }

  public Object enforceFieldType(final int index, final Object value) {
    Field field = fields.get(index);
    if (field.schema() == Schema.FLOAT64_SCHEMA) {
      return enforceDouble(value);
    } else if (field.schema() == Schema.INT64_SCHEMA) {
      return enforceLong(value);
    } else if (field.schema() == Schema.INT32_SCHEMA) {
      return enforceInteger(value);
    } else if (field.schema() == Schema.STRING_SCHEMA) {
      return enforceString(value);
    } else if (field.schema() == Schema.BOOLEAN_SCHEMA) {
      return enforceBoolean(value);
    } else {
      throw new KQLException("Type is not supported: " + field.schema());
    }
  }

  Double enforceDouble(final Object value) {
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
      throw new KQLException("Invalif field type. Value must be Double.");
    }
  }

  Long enforceLong(final Object value) {
    if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof Integer) {
      return ((Integer) value).longValue();
    } else if (value instanceof Long) {
      return ((Long) value).longValue();
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
      throw new KQLException("Invalif field type. Value must be Long.");
    }
  }

  Integer enforceInteger(final Object value) {

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
      throw new KQLException("Invalif field type. Value must be Integer.");
    }
  }

  String enforceString(final Object value) {
    if (value instanceof String || value instanceof CharSequence) {
      return value.toString();
    } else if (value == null) {
      return null;
    } else {
      throw new KQLException("Invalif field type. Value must be String.");
    }
  }

  Boolean enforceBoolean(final Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean(value.toString());
    } else if (value == null) {
      return null;
    } else {
      throw new KQLException("Invalif field type. Value must be Boolean.");
    }
  }
}
