/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.serde.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class KsqlJsonPojoDeserializer implements Deserializer<GenericRow> {


  //TODO: Possibily use Streaming API instead of ObjectMapper for better performance
  private ObjectMapper objectMapper = new ObjectMapper();

  private final Schema schema;
  private final Map<String, String> caseSensitiveKeyMap = new HashMap<>();

  /**
   * Default constructor needed by Kafka
   */
  public KsqlJsonPojoDeserializer(Schema schema) {
    this.schema = schema;
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    GenericRow data;
    try {
      data = getGenericRow(bytes);
    } catch (Exception e) {
      throw new SerializationException(e);
    }

    return data;
  }

  private GenericRow getGenericRow(byte[] rowJSONBytes) throws IOException {
    JsonNode jsonNode = objectMapper.readTree(rowJSONBytes);
    CaseInsensitiveJsonNode caseInsensitiveJsonNode = new CaseInsensitiveJsonNode(jsonNode);
    Map<String, String> keyMap = caseInsensitiveJsonNode.keyMap;
    List columns = new ArrayList();
    for (Field field: schema.fields()) {
      String jsonFieldName = field.name().substring(field.name().indexOf(".") + 1);
      JsonNode fieldJsonNode = jsonNode.get(keyMap.get(jsonFieldName));
      if (fieldJsonNode == null) {
        columns.add(null);
      } else {
        columns.add(enforceFieldType(field.schema(), fieldJsonNode));
      }

    }
    return new GenericRow(columns);
  }

  private Object enforceFieldType(Schema fieldSchema, JsonNode fieldJsonNode) {

    switch (fieldSchema.type()) {
      case BOOLEAN:
        return fieldJsonNode.asBoolean();
      case INT32:
        return fieldJsonNode.asInt();
      case INT64:
        return fieldJsonNode.asLong();
      case FLOAT64:
        return fieldJsonNode.asDouble();
      case STRING:
        if (fieldJsonNode.isTextual()) {
          return fieldJsonNode.asText();
        } else {
          return fieldJsonNode.toString();
        }
      case ARRAY:
        ArrayNode arrayNode = (ArrayNode) fieldJsonNode;
        Class elementClass = SchemaUtil.getJavaType(fieldSchema.valueSchema());
        Object[] arrayField =
            (Object[]) java.lang.reflect.Array.newInstance(elementClass, arrayNode.size());
        for (int i = 0; i < arrayNode.size(); i++) {
          arrayField[i] = enforceFieldType(fieldSchema.valueSchema(), arrayNode.get(i));
        }
        return arrayField;
      case MAP:
        Map<String, Object> mapField = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> iterator = fieldJsonNode.fields();
        while (iterator.hasNext()) {
          Map.Entry<String, JsonNode> entry = iterator.next();
          mapField.put(entry.getKey(), enforceFieldType(fieldSchema.valueSchema(), entry.getValue()));
        }
        return mapField;
      default:
        throw new KsqlException("Type is not supported: " + fieldSchema.type());

    }

  }

  class CaseInsensitiveJsonNode {
    JsonNode jsonNode;
    Map<String, String> keyMap = new HashMap<>();

    CaseInsensitiveJsonNode(JsonNode jsonNode) {
      this.jsonNode = jsonNode;
      Iterator<String> fieldNames = jsonNode.fieldNames();
      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        keyMap.put(fieldName.toUpperCase(), fieldName);
      }
    }

  }

  @Override
  public void close() {

  }
}
