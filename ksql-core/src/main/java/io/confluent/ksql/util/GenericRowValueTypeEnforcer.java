package io.confluent.ksql.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

public class GenericRowValueTypeEnforcer {

  Schema schema;
  List<Field> fields;

  public GenericRowValueTypeEnforcer(Schema schema) {
    this.schema = schema;
    this.fields = schema.fields();
  }

  public Object enforceFieldType(int index, Object value) {
    Field field = fields.get(index);
    if (field.schema().type() == Schema.Type.FLOAT64) {
      return enforceDouble(value);
    } else if (field.schema().type() == Schema.Type.INT64) {
      return enforceLong(value);
    } else if (field.schema().type() == Schema.Type.INT32) {
      return enforceInteger(value);
    } else if (field.schema().type() == Schema.Type.STRING) {
      return enforceString(value);
    } else if (field.schema().type() == Schema.Type.BOOLEAN) {
      return enforceBoolean(value);
    } else {
      throw new KSQLException("Type is not supported: "+field.schema().type());
    }
  }

  Double enforceDouble(Object value) {
    if (value instanceof Double) {
      return (Double) value;
    } else if (value instanceof Integer || value instanceof Long || value instanceof Float ||
               value instanceof Short || value instanceof Byte) {
      return (Double) value;
    } else if (value instanceof String || value instanceof CharSequence) {
      return Double.parseDouble(value.toString());
    } else {
      throw new KSQLException("Invalif field type. Value must be Double.");
    }
  }

  Long enforceLong(Object value) {
    if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
      return (Long) value;
    } else if (value instanceof String || value instanceof CharSequence) {
      return Long.parseLong(value.toString());
    } else {
      throw new KSQLException("Invalif field type. Value must be Long.");
    }
  }

  Integer enforceInteger(Object value) {
    if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
      return (Integer) value;
    } else if (value instanceof String || value instanceof CharSequence) {
      return Integer.parseInt(value.toString());
    } else {
      throw new KSQLException("Invalif field type. Value must be Integer.");
    }
  }

  String enforceString(Object value) {
    if (value instanceof String || value instanceof CharSequence) {
      return value.toString();
    } else {
      throw new KSQLException("Invalif field type. Value must be String.");
    }
  }

  Boolean enforceBoolean(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean(value.toString());
    } else {
      throw new KSQLException("Invalif field type. Value must be Boolean.");
    }
  }
}
