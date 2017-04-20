/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import java.util.Map;

public class WindowedSerde implements Serde<Windowed<String>> {

  final private Serializer<Windowed<String>> serializer;
  final private Deserializer<Windowed<String>> deserializer;

  public WindowedSerde() {
    serializer = new WindowedSerializer<>(new StringSerializer());
    deserializer = new WindowedDeserializer<>(new StringDeserializer());
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    serializer.configure(configs, isKey);
    deserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    serializer.close();
    deserializer.close();
  }

  @Override
  public Serializer<Windowed<String>> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<Windowed<String>> deserializer() {
    return deserializer;
  }
}
