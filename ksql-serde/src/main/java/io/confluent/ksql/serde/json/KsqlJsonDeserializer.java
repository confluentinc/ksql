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

  /**
   * Default constructor needed by Kafka
   */
  public KsqlJsonDeserializer(Schema schema, boolean isInternal) {
    if (isInternal) {
      this.schema = schema;
    } else {
      this.schema = SchemaUtil.getSchemaWithNoAlias(schema);
    }

  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
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
  private GenericRow getGenericRow(byte[] rowJsonBytes) throws IOException {
    JsonNode jsonNode = objectMapper.readTree(rowJsonBytes);
    CaseInsensitiveJsonNode caseInsensitiveJsonNode = new CaseInsensitiveJsonNode(jsonNode);

    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(Collections.singletonMap("schemas.enable", false), false);
    SchemaAndValue schemaAndValue = jsonConverter.toConnectData("topic", rowJsonBytes);
    Map valueMap = (Map) schemaAndValue.value();

    Map<String, String> keyMap = caseInsensitiveJsonNode.keyMap;
    List columns = new ArrayList();
    for (Field field : schema.fields()) {
      Object columnVal = valueMap
          .get(keyMap.get(field.name()));
      columns.add(enforceFieldType(field.schema(), columnVal));

    }
    return new GenericRow(columns);
  }

  // This is a temporary requirement until we can ensure that the types that Connect JSON
  // convertor creates are supported in KSQL.
  private Object enforceFieldType(Schema fieldSchema, Object columnVal) {
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
        List arrayList = (List) columnVal;
        List columnArrayList = new ArrayList();
        arrayList
            .stream()
            .forEach(object -> columnArrayList
                .add(enforceFieldType(fieldSchema.valueSchema(), object)));

        return columnArrayList;
      case MAP:
        Map map = (Map) columnVal;
        Map columnMap = new HashMap();
        map.forEach((k, v) -> columnMap.put(enforceFieldType(Schema.STRING_SCHEMA, k),
                                            enforceFieldType(fieldSchema.valueSchema(), v)));
        return map;
      case STRUCT:
        Map structMap = (Map) columnVal;
        Struct columnStruct = new Struct(fieldSchema);
        Map<String, String> caseInsensitiveFieldNameMap =
            getCaseInsensitiveFieldNameMap(fieldSchema.fields());
        fieldSchema.fields()
            .stream()
            .forEach(
                field -> columnStruct.put(field.name(),
                                          enforceFieldType(
                                              field.schema(), structMap.get(
                                                  caseInsensitiveFieldNameMap.get(field.name())
                                              ))));
        return columnStruct;
      default:
        throw new KsqlException("Type is not supported: " + fieldSchema.type());
    }
  }


  private Map<String, String> getCaseInsensitiveFieldNameMap(List<Field> fields) {
    Map<String, String> fieldNameMap = new HashMap<>();
    fields.stream().forEach(field -> fieldNameMap.put(field.name().toUpperCase(), field.name()));
    return fieldNameMap;
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