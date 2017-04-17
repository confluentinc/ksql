/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.serde.json;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kql.physical.GenericRow;

public class KQLJsonPOJOSerializer implements Serializer<GenericRow> {

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final Schema schema;

//  private Class<T> tClass;

  /**
   * Default constructor needed by Kafka
   */
  public KQLJsonPOJOSerializer(Schema schema) {
    this.schema = schema;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(final Map<String, ?> props, final boolean isKey) {
//    tClass = (Class<T>) props.get("JsonPOJOClass");
  }

  @Override
  public byte[] serialize(final String topic, final GenericRow data) {
    if (data == null) {
      return null;
    }

    try {
      Map map = new HashMap();
      for (int i = 0; i < data.getColumns().size(); i++) {
        String jsonFieldName = schema.fields().get(i).name().substring(schema.fields().get(i).name().indexOf(".")+1).toLowerCase();
        map.put(jsonFieldName, data.getColumns().get(i));
      }
//      byte[] b = objectMapper.valueToTree(map).binaryValue();
      byte[] b = objectMapper.writeValueAsBytes(map);
//      byte[] b = objectMapper.writeValueAsBytes(data);
      System.out.println(new String(b));
      return b;
    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON message", e);
    }
  }

  @Override
  public void close() {
  }

}
