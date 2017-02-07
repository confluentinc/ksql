/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.serde.json;


import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KQLJsonPOJOSerializer<T> implements Serializer<T> {

  private final ObjectMapper objectMapper = new ObjectMapper();

  private Class<T> tClass;

  /**
   * Default constructor needed by Kafka
   */
  public KQLJsonPOJOSerializer() {

  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(final Map<String, ?> props, final boolean isKey) {
    tClass = (Class<T>) props.get("JsonPOJOClass");
  }

  @Override
  public byte[] serialize(final String topic, final T data) {
    if (data == null) {
      return null;
    }

    try {
      return objectMapper.writeValueAsBytes(data);
    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON message", e);
    }
  }

  @Override
  public void close() {
  }

}
