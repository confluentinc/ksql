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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.util.SerdeUtils;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

public class KsqlJsonDeserializer implements Deserializer<GenericRow> {

  //TODO: Possibily use Streaming API instead of ObjectMapper for better performance
  private ObjectMapper objectMapper = new ObjectMapper();

  private final Schema schema;
  private final JsonConverter jsonConverter;

  /**
   * Default constructor needed by Kafka
   */
  public KsqlJsonDeserializer(final Schema schema, final boolean isInternal) {
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
  public void configure(final Map<String, ?> map, boolean b) {
  }

  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try {
      return getGenericRow(bytes);
    } catch (Exception e) {
      throw new SerializationException(
          "KsqlJsonDeserializer failed to deserialize data for topic: " + topic,
          e
      );
    }
  }

  @SuppressWarnings("unchecked")
  private GenericRow getGenericRow(final byte[] rowJsonBytes) throws IOException {
    final JsonNode jsonNode = objectMapper.readTree(rowJsonBytes);
    final CaseInsensitiveJsonNode caseInsensitiveJsonNode = new CaseInsensitiveJsonNode(jsonNode);

    final SchemaAndValue schemaAndValue = jsonConverter.toConnectData("topic", rowJsonBytes);
    final Map<String, Object> valueMap = (Map) schemaAndValue.value();
    if (valueMap == null) {
      return null;
    }

    final  Map<String, String> keyMap = caseInsensitiveJsonNode.keyMap;
    final List<Object> columns = new ArrayList();
    for (Field field : schema.fields()) {
      final Object columnVal = valueMap
          .get(keyMap.get(field.name()));
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
        return columnVal.toString();
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

  private List<?> enforceFieldTypeForArray(final Schema fieldSchema, final List<?> arrayList) {
    return arrayList.stream()
        .map(item -> enforceFieldType(fieldSchema.valueSchema(), item))
        .collect(Collectors.toList());
  }

  private Map<String, Object> enforceFieldTypeForMap(
      final Schema fieldSchema,
      final Map<String, Object> columnMap) {
    return columnMap.entrySet().stream()
        .collect(Collectors.toMap(
            e -> enforceFieldType(Schema.OPTIONAL_STRING_SCHEMA, e.getKey()).toString(),
            e -> enforceFieldType(fieldSchema.valueSchema(), e.getValue())
        ));
  }

  private Struct enforceFieldTypeForStruct(
      final Schema fieldSchema,
      final Map<String, Object> structMap) {
    final Struct columnStruct = new Struct(fieldSchema);
    final Map<String, String> caseInsensitiveFieldNameMap =
        getCaseInsensitiveFieldNameMap(fieldSchema.fields());
    final Map<String, String> caseInsensitiveStructFieldNameMap =
        getCaseInsensitiveStructFieldNameMap(structMap);
    fieldSchema.fields()
        .forEach(
            field -> columnStruct.put(field.name(),
                enforceFieldType(
                    field.schema(), structMap.get(
                        caseInsensitiveStructFieldNameMap.get(field.name().toUpperCase())
                    ))));
    return columnStruct;
  }

  private Map<String, String> getCaseInsensitiveFieldNameMap(final List<Field> fields) {
    return fields.stream()
        .map(Field::name)
        .collect(Collectors.toMap(
            String::toUpperCase,
            name -> name));
  }

  private Map<String, String> getCaseInsensitiveStructFieldNameMap(
      final Map<String, Object> structMap
  ) {
    return structMap.entrySet().stream()
        .collect(Collectors.toMap(
            e -> e.getKey().toUpperCase(),
            Entry::getKey
        ));
  }

  static class CaseInsensitiveJsonNode {

    Map<String, String> keyMap = new HashMap<>();

    CaseInsensitiveJsonNode(JsonNode jsonNode) {
      Iterator<String> fieldNames = jsonNode.fieldNames();
      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        if (fieldName.startsWith("@")) {
          if (fieldName.length() == 1) {
            throw new KsqlException("Field name cannot be '@'.");
          }
          keyMap.put(fieldName.toUpperCase().substring(1), fieldName);
        } else {
          keyMap.put(fieldName.toUpperCase(), fieldName);
        }

      }
    }

  }


  @Override
  public void close() {

  }
}