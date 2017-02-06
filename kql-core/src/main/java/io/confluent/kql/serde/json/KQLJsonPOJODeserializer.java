/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.kql.serde.json;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KQLJsonPOJODeserializer<T> implements Deserializer<T> {

  private ObjectMapper objectMapper = new ObjectMapper();

  private Class<T> tClass;

  /**
   * Default constructor needed by Kafka
   */
  public KQLJsonPOJODeserializer() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(final Map<String, ?> props, final boolean isKey) {
    tClass = (Class<T>) props.get("JsonPOJOClass");
  }

  @Override
  public T deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    T data;
    try {
      data = objectMapper.readValue(bytes, tClass);
    } catch (Exception e) {
      throw new SerializationException(e);
    }

    return data;
  }

  @Override
  public void close() {

  }
}
