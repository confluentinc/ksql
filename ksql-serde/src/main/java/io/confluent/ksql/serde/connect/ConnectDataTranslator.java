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

package io.confluent.ksql.serde.connect;

import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

public class ConnectDataTranslator implements DataTranslator {
  private static final String PATH_SEPARATOR = "->";

  private final Schema schema;

  public ConnectDataTranslator(final Schema schema) {
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  @Override
  public Struct toKsqlRow(final Schema connectSchema, final Object connectData) {
    if (connectData == null) {
      return null;
    }

    if (connectSchema.type() == Type.STRUCT) {
      return (Struct) toKsqlValue(schema, connectSchema, connectData, "");
    }

    if (schema.fields().size() != 1) {
      throw new KsqlException("Expected Avro record not primitive, array or map type");
    }

    final Struct struct = new Struct(schema);
    final Field field = schema.fields().get(0);

    final Object coerced = toKsqlValue(field.schema(), connectSchema, connectData, "");
    struct.put(field, coerced);

    return struct;
  }

  private static void throwTypeMismatchException(
      final String pathStr,
      final Schema schema,
      final Schema connectSchema
  ) {
    throw new DataException(
        String.format(
            "Cannot deserialize type %s as type %s for field %s",
            connectSchema.type().getName(),
            schema.type().getName(),
            pathStr));
  }

  private static void validateType(
      final String pathStr,
      final Schema schema,
      final Schema connectSchema,
      final Schema.Type[] validTypes
  ) {
    // don't use stream here
    for (final Schema.Type type : validTypes) {
      if (connectSchema.type().equals(type)) {
        return;
      }
    }
    throwTypeMismatchException(pathStr, schema, connectSchema);
  }

  private static void validateType(
      final String pathStr,
      final Schema schema,
      final Schema connectSchema
  ) {
    if (!connectSchema.type().equals(schema.type())) {
      throwTypeMismatchException(pathStr, schema, connectSchema);
    }
  }

  // use static arrays instead of varargs from validateSchema. Under the hood
  // varargs creates and populates arrays on each call, which is expensive.
  private static final Schema.Type[] INT64_ACCEPTABLE_TYPES = {
      Schema.Type.INT64,
      Schema.Type.INT32,
      Schema.Type.INT16,
      Schema.Type.INT8
  };

  private static final Schema.Type[] INT32_ACCEPTABLE_TYPES = {
      Schema.Type.INT32,
      Schema.Type.INT16,
      Schema.Type.INT8
  };

  private static final Schema.Type[] FLOAT64_ACCEPTABLE_TYPES = {
      Schema.Type.FLOAT32,
      Schema.Type.FLOAT64
  };

  private static final Schema.Type[] STRING_ACCEPTABLE_TYPES = {
      Schema.Type.INT8,
      Schema.Type.INT16,
      Schema.Type.INT32,
      Schema.Type.INT64,
      Schema.Type.BOOLEAN,
      Schema.Type.STRING
  };

  private static void validateSchema(
      final String pathStr,
      final Schema schema,
      final Schema connectSchema
  ) {
    switch (schema.type()) {
      case BOOLEAN:
      case ARRAY:
      case MAP:
      case STRUCT:
        validateType(pathStr, schema, connectSchema);
        break;
      case STRING:
        validateType(pathStr, schema, connectSchema, STRING_ACCEPTABLE_TYPES);
        break;
      case INT64:
        validateType(pathStr, schema, connectSchema, INT64_ACCEPTABLE_TYPES);
        break;
      case INT32:
        validateType(pathStr, schema, connectSchema, INT32_ACCEPTABLE_TYPES);
        break;
      case FLOAT64:
        validateType(pathStr, schema, connectSchema, FLOAT64_ACCEPTABLE_TYPES);
        break;
      default:
        throw new RuntimeException(
            "Unexpected data type seen in schema: " + schema.type().getName());
    }
  }

  private static Object maybeConvertLogicalType(
      final Schema connectSchema,
      final Object connectValue
  ) {
    if (connectSchema.name() == null) {
      return connectValue;
    }
    switch  (connectSchema.name()) {
      case Date.LOGICAL_NAME:
        return Date.fromLogical(connectSchema, (java.util.Date) connectValue);
      case Time.LOGICAL_NAME:
        return Time.fromLogical(connectSchema, (java.util.Date) connectValue);
      case Timestamp.LOGICAL_NAME:
        return Timestamp.fromLogical(connectSchema, (java.util.Date) connectValue);
      default:
        return connectValue;
    }
  }

  @SuppressWarnings("unchecked")
  private Object toKsqlValue(
      final Schema schema,
      final Schema connectSchema,
      final Object connectValue,
      final String pathStr
  ) {
    // Map a connect value+schema onto the schema expected by KSQL. For now this involves:
    // - handling case insensitivity for struct field names
    // - setting missing values to null
    if (connectSchema == null) {
      return null;
    }
    validateSchema(pathStr, schema, connectSchema);
    if (connectValue == null) {
      return null;
    }
    final Object convertedValue = maybeConvertLogicalType(connectSchema, connectValue);
    switch (schema.type()) {
      case INT64:
        return ((Number) convertedValue).longValue();
      case INT32:
        return ((Number) convertedValue).intValue();
      case FLOAT64:
        return ((Number) convertedValue).doubleValue();
      case ARRAY:
        return toKsqlArray(
            schema.valueSchema(), connectSchema.valueSchema(), (List) convertedValue, pathStr);
      case MAP:
        return toKsqlMap(
            schema.keySchema(), connectSchema.keySchema(),
            schema.valueSchema(), connectSchema.valueSchema(), (Map) convertedValue, pathStr);
      case STRUCT:
        return toKsqlStruct(schema, connectSchema, (Struct) convertedValue, pathStr);
      case STRING:
        // use String.valueOf to convert various int types and Boolean to string
        return String.valueOf(convertedValue);
      default:
        return convertedValue;
    }
  }

  private List<?> toKsqlArray(
      final Schema valueSchema,
      final Schema connectValueSchema,
      final List<Object> connectArray,
      final String pathStr
  ) {
    final List<Object> ksqlArray = new ArrayList<>(connectArray.size());
    // streams are expensive, so we don't use them from serdes.
    // build the array using forEach instead.
    connectArray.forEach(
        item -> ksqlArray.add(
            toKsqlValue(
                valueSchema, connectValueSchema, item, pathStr + PATH_SEPARATOR + "ARRAY")));
    return ksqlArray;
  }

  private Map<?, ?> toKsqlMap(
      final Schema keySchema,
      final Schema connectKeySchema,
      final Schema valueSchema,
      final Schema connectValueSchema,
      final Map<Object, Object> connectMap,
      final String pathStr
  ) {
    final Map<Object, Object> ksqlMap = new HashMap<>();
    // streams are expensive, so we don't use them from serdes.
    // build the map using forEach instead.
    connectMap.forEach(
        (key, value) -> ksqlMap.put(
            toKsqlValue(
                keySchema,
                connectKeySchema,
                key,
                pathStr + PATH_SEPARATOR + "MAP_KEY"),
            toKsqlValue(
                valueSchema,
                connectValueSchema,
                value,
                pathStr + PATH_SEPARATOR + "MAP_VAL")
        )
    );
    return ksqlMap;
  }

  private Struct toKsqlStruct(
      final Schema schema,
      final Schema connectSchema,
      final Struct connectStruct,
      final String pathStr
  ) {
    // todo: check name here? e.g. what if the struct gets changed to a union?
    final Struct ksqlStruct = new Struct(schema);
    final Map<String, Field> caseInsensitiveFieldMap =
        getCaseInsensitiveFieldMap(connectSchema);
    schema.fields().forEach(field -> {
      final String fieldNameUppercase = field.name().toUpperCase();
      // TODO: should we throw an exception if this is not true? this means the schema changed
      //       or the user declared the source with a schema incompatible with the registry schema
      if (caseInsensitiveFieldMap.containsKey(fieldNameUppercase)) {
        final Field connectField = caseInsensitiveFieldMap.get(fieldNameUppercase);
        // make sure to get/put the field using the Field object to avoid a lookup in Struct
        final Object fieldValue = connectStruct.get(connectField);
        final Schema fieldSchema = connectField.schema();
        ksqlStruct.put(
            field,
            toKsqlValue(
                field.schema(),
                fieldSchema,
                fieldValue,
                pathStr + PATH_SEPARATOR + field.name()));
      }
    });
    return ksqlStruct;
  }

  private static Map<String, Field> getCaseInsensitiveFieldMap(final Schema schema) {
    final Map<String, Field> fieldsByName = new HashMap<>();
    schema.fields().forEach(
        field -> fieldsByName.put(field.name().toUpperCase(), field)
    );
    return fieldsByName;
  }

  public Object toConnectRow(final Struct struct) {
    if (schema.type() == Type.STRUCT) {
      return toConnectStruct(struct);
    }

    if (struct.schema().fields().size() != 1) {
      throw new SerializationException("Expected to serialize primitive, map or array not record");
    }

    final Field field = struct.schema().fields().get(0);
    return struct.get(field);
  }

  private Object toConnectStruct(final Struct row) {
    final Struct struct = new Struct(schema);

    final Iterator<Field> ksqlIt = row.schema().fields().iterator();

    for (final Field connectField : schema.fields()) {
      final Field ksqlField = ksqlIt.next();
      final Object value = row.get(ksqlField);
      struct.put(connectField, value);
    }

    return struct;
  }
}
