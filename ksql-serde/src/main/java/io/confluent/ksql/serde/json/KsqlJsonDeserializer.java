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

package io.confluent.ksql.serde.json;

import com.google.gson.Gson;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.util.SerdeUtils;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

public class KsqlJsonDeserializer implements Deserializer<GenericRow> {
  private final Schema schema;
  private final JsonConverter jsonConverter;

  private final Gson gson;

  /**
   * Default constructor needed by Kafka
   */
  public KsqlJsonDeserializer(final Schema schema, final boolean isInternal) {
    gson = new Gson();
    // If this is a Deserializer for an internal topic in the streams app
    if (isInternal) {
      this.schema = schema;
    } else {
      this.schema = SchemaUtil.getSchemaWithNoAlias(schema);
    }
    jsonConverter = new JsonConverter();
    jsonConverter.configure(Collections.singletonMap("schemas.enable", false), false);
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try {
      return getGenericRow(bytes);
    } catch (final Exception e) {
      throw new SerializationException(
          "KsqlJsonDeserializer failed to deserialize data for topic: " + topic,
          e
      );
    }
  }

  @SuppressWarnings("unchecked")
  private GenericRow getGenericRow(final byte[] rowJsonBytes) {
    final SchemaAndValue schemaAndValue = jsonConverter.toConnectData("topic", rowJsonBytes);
    final Map<String, Object> valueMap = (Map) schemaAndValue.value();
    if (valueMap == null) {
      return null;
    }

    final Map<String, String> caseInsensitiveFieldNameMap =
        getCaseInsensitiveFieldNameMap(valueMap, true);

    final List<Object> columns = new ArrayList(schema.fields().size());
    for (final Field field : schema.fields()) {
      final Object columnVal = valueMap.get(caseInsensitiveFieldNameMap.get(field.name()));
      columns.add(enforceFieldType(field.schema(), columnVal));
    }
    return new GenericRow(columns);
  }

  // This is a temporary requirement until we can ensure that the types that Connect JSON
  // convertor creates are supported in KSQL.
  private Object enforceFieldType(final Schema fieldSchema, final Object columnVal) {
    if (columnVal == null) {
      return null;
    }
    switch (fieldSchema.type()) {
      case BOOLEAN:
        return SerdeUtils.toBoolean(columnVal);
      case INT32:
        return SerdeUtils.toInteger(columnVal);
      case INT64:
        return SerdeUtils.toLong(columnVal);
      case FLOAT64:
        return SerdeUtils.toDouble(columnVal);
      case STRING:
        return processString(columnVal);
      case ARRAY:
        return enforceFieldTypeForArray(fieldSchema, (List<?>) columnVal);
      case MAP:
        return enforceFieldTypeForMap(fieldSchema, (Map<String, Object>) columnVal);
      case STRUCT:
        return enforceFieldTypeForStruct(fieldSchema, (Map<String, Object>) columnVal);
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

  private List<?> enforceFieldTypeForArray(final Schema fieldSchema, final List<?> arrayList) {
    final List<Object> array = new ArrayList<>(arrayList.size());
    for (final Object item : arrayList) {
      array.add(enforceFieldType(fieldSchema.valueSchema(), item));
    }
    return array;
  }

  private Map<String, Object> enforceFieldTypeForMap(
      final Schema fieldSchema,
      final Map<String, ?> columnMap) {
    final Map<String, Object> ksqlMap = new HashMap<>();
    for (final Map.Entry<String, ?> e : columnMap.entrySet()) {
      ksqlMap.put(
          enforceFieldType(Schema.OPTIONAL_STRING_SCHEMA, e.getKey()).toString(),
          enforceFieldType(fieldSchema.valueSchema(), e.getValue())
      );
    }
    return ksqlMap;
  }

  private Struct enforceFieldTypeForStruct(
      final Schema fieldSchema,
      final Map<String, Object> structMap) {
    final Struct columnStruct = new Struct(fieldSchema);
    final Map<String, String> caseInsensitiveStructFieldNameMap =
        getCaseInsensitiveFieldNameMap(structMap, false);
    fieldSchema.fields()
        .forEach(
            field -> columnStruct.put(field.name(),
                enforceFieldType(
                    field.schema(), structMap.get(
                        caseInsensitiveStructFieldNameMap.get(field.name().toUpperCase())
                    ))));
    return columnStruct;
  }

  private Map<String, String> getCaseInsensitiveFieldNameMap(final Map<String, ?> map,
                                                             final boolean omitAt) {
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
