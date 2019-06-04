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

package io.confluent.ksql.serde.json;

import com.google.gson.Gson;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlJsonDeserializer implements Deserializer<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(KsqlJsonDeserializer.class);

  private final Gson gson;
  private final Schema schema;
  private final JsonConverter jsonConverter;
  private final ProcessingLogger recordLogger;

  public KsqlJsonDeserializer(
      final Schema schema,
      final ProcessingLogger recordLogger
  ) {
    this.gson = new Gson();
    this.schema = Objects.requireNonNull(schema, "schema");
    this.jsonConverter = new JsonConverter();
    this.jsonConverter.configure(Collections.singletonMap("schemas.enable", false), false);
    this.recordLogger = Objects.requireNonNull(recordLogger, "recordLogger");

    if (schema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("KSQL expects all top level schemas to be STRUCTs");
    }
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public Struct deserialize(final String topic, final byte[] bytes) {
    try {
      final Struct row = deserialize(bytes);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Deserialized row. topic:{}, row:{}", topic, row);
      }
      return row;
    } catch (final Exception e) {
      recordLogger.error(
          SerdeProcessingLogMessageFactory.deserializationErrorMsg(
              e,
              Optional.ofNullable(bytes))
      );
      throw new SerializationException(
          "KsqlJsonDeserializer failed to deserialize data for topic: " + topic, e);
    }
  }

  @SuppressWarnings("unchecked")
  private Struct deserialize(final byte[] bytes) {
    final SchemaAndValue schemaAndValue = jsonConverter.toConnectData("topic", bytes);
    final Object value = schemaAndValue.value();
    if (value == null) {
      return null;
    }

    final Map<String, Object> map = (Map<String, Object>) value;
    final Map<String, String> caseInsensitiveFieldNameMap =
        getCaseInsensitiveFieldNameMap(map, true);

    final Struct struct = new Struct(schema);
    for (final Field field : schema.fields()) {
      final Object columnVal = map.get(caseInsensitiveFieldNameMap.get(field.name()));
      final Object coerced = enforceFieldType(field.schema(), columnVal);
      struct.put(field, coerced);
    }
    return struct;
  }

  // This is a temporary requirement until we can ensure that the types that Connect JSON
  // convertor creates are supported in KSQL.
  private Object enforceFieldType(final Schema fieldSchema, final Object columnVal) {
    if (columnVal == null) {
      return null;
    }
    switch (fieldSchema.type()) {
      case BOOLEAN:
        return JsonSerdeUtils.toBoolean(columnVal);
      case INT32:
        return JsonSerdeUtils.toInteger(columnVal);
      case INT64:
        return JsonSerdeUtils.toLong(columnVal);
      case FLOAT64:
        return JsonSerdeUtils.toDouble(columnVal);
      case STRING:
        return processString(columnVal);
      case ARRAY:
        return enforceFieldTypeForArray(fieldSchema, columnVal);
      case MAP:
        return enforceFieldTypeForMap(fieldSchema, columnVal);
      case STRUCT:
        return enforceFieldTypeForStruct(fieldSchema, columnVal);
      default:
        throw new KsqlException("Type is not supported: " + fieldSchema.type());
    }
  }

  private String processString(final Object columnVal) {
    if (columnVal instanceof Map) {
      return gson.toJson(columnVal);
    }
    return columnVal.toString();
  }

  private List<?> enforceFieldTypeForArray(final Schema fieldSchema, final Object value) {
    if (!(value instanceof List)) {
      throw new IllegalArgumentException("value is not a list. "
          + "type: " + value.getClass()
          + ", fieldSchema: " + fieldSchema);
    }

    final List<?> list = (List<?>) value;
    final List<Object> array = new ArrayList<>(list.size());
    for (final Object item : list) {
      array.add(enforceFieldType(fieldSchema.valueSchema(), item));
    }
    return array;
  }

  private Map<String, Object> enforceFieldTypeForMap(final Schema fieldSchema, final Object value) {
    if (!(value instanceof Map)) {
      throw new IllegalArgumentException("value is not a map. "
          + "type: " + value.getClass()
          + ", fieldSchema: " + fieldSchema);
    }

    final Map<?, ?> map = (Map<?, ?>) value;
    final Map<String, Object> ksqlMap = new HashMap<>(map.size());
    for (final Map.Entry<?, ?> e : map.entrySet()) {
      ksqlMap.put(
          enforceFieldType(Schema.OPTIONAL_STRING_SCHEMA, e.getKey()).toString(),
          enforceFieldType(fieldSchema.valueSchema(), e.getValue())
      );
    }
    return ksqlMap;
  }

  @SuppressWarnings("unchecked")
  private Struct enforceFieldTypeForStruct(final Schema fieldSchema, final Object value) {
    if (!(value instanceof Map)) {
      throw new IllegalArgumentException("value is not a struct. "
          + "type: " + value.getClass()
          + ", fieldSchema: " + fieldSchema);
    }

    final Map<String, ?> map = (Map<String, ?>) value;
    final Struct columnStruct = new Struct(fieldSchema);
    final Map<String, String> caseInsensitiveStructFieldNameMap =
        getCaseInsensitiveFieldNameMap(map, false);
    fieldSchema.fields()
        .forEach(
            field -> columnStruct.put(field.name(),
                enforceFieldType(
                    field.schema(), map.get(
                        caseInsensitiveStructFieldNameMap.get(field.name().toUpperCase())
                    ))));
    return columnStruct;
  }

  private static Map<String, String> getCaseInsensitiveFieldNameMap(
      final Map<String, ?> map,
      final boolean omitAt
  ) {
    final Map<String, String> keyMap = new HashMap<>();
    for (final Map.Entry<String, ?> entry : map.entrySet()) {
      if (omitAt && entry.getKey().startsWith("@")) {
        if (entry.getKey().length() == 1) {
          throw new KsqlException("Field name cannot be '@'.");
        }
        keyMap.put(entry.getKey().toUpperCase().substring(1), entry.getKey());
      } else {
        keyMap.put(entry.getKey().toUpperCase(), entry.getKey());
      }
    }
    return keyMap;
  }

  @Override
  public void close() {
  }
}
