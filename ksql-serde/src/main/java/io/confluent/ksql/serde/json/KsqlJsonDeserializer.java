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
import java.util.Map.Entry;
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

public class KsqlJsonDeserializer implements Deserializer<Struct> {

  private static final Logger LOG = LoggerFactory.getLogger(KsqlJsonDeserializer.class);

  private final Gson gson;
  private final Schema schema;
  private final JsonConverter jsonConverter;
  private final ProcessingLogger recordLogger;
  private final boolean ambiguousMapSchema;

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

    this.ambiguousMapSchema = schema.fields().size() == 1
        && schema.fields().get(0).schema().type() == Type.MAP
        && schema.fields().get(0).schema().keySchema().type() == Type.STRING;
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

    if (value instanceof Map) {
      final Map<String, Object> map = (Map<String, Object>) value;
      if (treatAsRow(map)) {
        return asRow(map);
      }
    }

    if (schema.fields().size() != 1) {
      throw new KsqlException("Expected JSON object not JSON value or array");
    }

    final Struct struct = new Struct(schema);
    final Field field = schema.fields().get(0);

    final Object coerced = enforceFieldType(field.schema(), value);
    struct.put(field, coerced);

    return struct;
  }

  /**
   * Should this value be treated as a record or a top level map?
   *
   * <p>The deserializer supports deserializing top-level unnamed primitives, arrays and maps if
   * the target schema only contains a single field of the required type, e.g. the value '10' can be
   * deserialized into a schema containing a single numeric field. Likewise, the value '[1,2]' can
   * be deserialized into a schema containing a single array field. The same is true of maps, but
   * maps can be ambiguous.
   *
   * <p>There is ambiguity when handling top level maps due to the fact that JSON does not have a
   * map type, so KSQL persists maps as JSON objects.  JSON objects are also used to represent KSQL
   * structs. When the deserializer is presented with a JSON object and a target schema that
   * contains a single MAP field, it is not immediately apparent if this should be deserialized as a
   * struct or a top-level map.
   *
   * <p>Map fields are only ambiguous if:
   * <ul>
   *   <li>The target schema contains only a single field</li>
   *   <li>and that field is a map</li>
   *   <li>and that map has a string key</li>
   *   <li>and that map has a key with the same name as the field</li>
   * </ul>
   *
   * <p>This method determines which it should be treated as by comparing the target schema with
   * the actual value. If the value is coercible into the target schema then it will be treated
   * as a record, not a top level map.
   *
   * <p>Note: It is strongly recommended that users avoid the situation by ensuring that single
   * map fields never have the same name as any of the keys in the map.
   *
   * @param map the map to check.
   * @return true if it should be treated as a record.
   */
  private boolean treatAsRow(final Map<String, Object> map) {
    if (!ambiguousMapSchema) {
      return true;
    }

    final Field onlyField = schema.fields().get(0);
    final String fieldName = onlyField.name();

    final boolean coercibleToRow = map.entrySet().stream()
        .filter(e -> fieldName.equalsIgnoreCase(e.getKey()))
        .map(Entry::getValue)
        .filter(v -> JsonSerdeUtils.isCoercible(v, onlyField.schema()))
        .map(v -> true)
        .findFirst()
        .orElse(false);

    final boolean coercibleToField = JsonSerdeUtils.isCoercible(map, onlyField.schema());

    return coercibleToRow || !coercibleToField;
  }

  private Struct asRow(final Map<String, Object> valueMap) {
    final Map<String, String> caseInsensitiveFieldNameMap =
        getCaseInsensitiveFieldNameMap(valueMap, true);

    final Struct struct = new Struct(schema);
    for (final Field field : schema.fields()) {
      final Object columnVal = valueMap.get(caseInsensitiveFieldNameMap.get(field.name()));
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
