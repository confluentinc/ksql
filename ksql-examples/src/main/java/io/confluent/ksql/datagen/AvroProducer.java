/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.datagen;

import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.avro.KsqlGenericRowAvroSerializer;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class AvroProducer extends DataGenProducer {

  @Override
  protected Serializer<GenericRow> getSerializer(
      Schema avroSchema,
      org.apache.kafka.connect.data.Schema kafkaSchema,
      String topicName
  ) {
    Serializer<GenericRow> result = new KsqlGenericRowAvroSerializer(kafkaSchema);
    Map<String, String> serializerConfiguration = new HashMap<>();
    serializerConfiguration.put(KsqlGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, avroSchema.toString());
    result.configure(serializerConfiguration, false);
    return result;
  }
}