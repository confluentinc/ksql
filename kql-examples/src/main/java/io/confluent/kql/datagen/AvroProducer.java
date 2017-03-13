/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.datagen;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.serde.avro.KQLGenericRowAvroSerializer;

import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class AvroProducer extends DataGenProducer {
  @Override
  protected Serializer<GenericRow> getSerializer(Schema schema) {
    Serializer<GenericRow> result = new KQLGenericRowAvroSerializer();
    Map<String, String> serializerConfiguration = new HashMap<>();
    serializerConfiguration.put(KQLGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, schema.toString());
    result.configure(serializerConfiguration, false);
    return result;
  }
}