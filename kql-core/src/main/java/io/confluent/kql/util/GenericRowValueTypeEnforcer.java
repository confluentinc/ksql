/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GenericRowValueTypeEnforcer {

  Schema schema;
  List<Field> fields;

  public GenericRowValueTypeEnforcer(final Schema schema) {
    this.schema = schema;
    this.fields = schema.fields();
  }

  public Object enforceFieldType(final int index, final Object value) {
    Field field = fields.get(index);
    return enforceFieldType(field.schema(), value);
//    if (field.schema() == Schema.FLOAT64_SCHEMA) {
//      return enforceDouble(value);
//    } else if (field.schema() == Schema.INT64_SCHEMA) {
//      return enforceLong(value);
//    } else if (field.schema() == Schema.INT32_SCHEMA) {
//      return enforceInteger(value);
//    } else if (field.schema() == Schema.STRING_SCHEMA) {
//      return enforceString(value);
//    } else if (field.schema() == Schema.BOOLEAN_SCHEMA) {
//      return enforceBoolean(value);
//    } else if (field.schema().type() == Schema.Type.ARRAY) {
//      return enforceFieldType(field.schema(), value);
//    } else {
//      throw new KQLException("Type is not supported: " + field.schema());
//    }
  }

  public Object enforceFieldType(Schema schema, final Object value) {
    if (schema == Schema.FLOAT64_SCHEMA) {
      return enforceDouble(value);
    } else if (schema == Schema.INT64_SCHEMA) {
      return enforceLong(value);
    } else if (schema == Schema.INT32_SCHEMA) {
      return enforceInteger(value);
    } else if (schema == Schema.STRING_SCHEMA) {
      return enforceString(value);
    } else if (schema == Schema.BOOLEAN_SCHEMA) {
      return enforceBoolean(value);
    } else if (schema.type() == Schema.Type.ARRAY) {
//      List array = (List) value;
////      Object[] arrayObjects = new Object[array.size()];
//      Object[] arrayObjects = (Object[]) java.lang.reflect.Array.newInstance(SchemaUtil
//                                                                                 .getJavaType(schema.valueSchema()), array.size());
//      for (int i = 0; i < array.size(); i++) {
//        arrayObjects[i] = enforceFieldType(schema.valueSchema(), array.get(i));
//      }
//      return arrayObjects;
      return value;
    } else if (schema.type() == Schema.Type.MAP) {
//      LinkedHashMap valueMap = (LinkedHashMap) value;
//      // No need to keep it as LinkedHashMap.
//      Map<String, Object> map = new HashMap<>();
//      for (Object key: valueMap.keySet()) {
//        map.put(String.valueOf(key), valueMap.get(key));
//      }
//      return map;
      return value;
    } else {
      throw new KQLException("Type is not supported: " + schema);
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
