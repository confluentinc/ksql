/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.datagen;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.serde.csv.KQLCsvSerializer;

import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serializer;

public class CsvProducer extends DataGenProducer {

  @Override
  protected Serializer<GenericRow> getSerializer(
      Schema avroSchema,
      org.apache.kafka.connect.data.Schema kafkaSchema,
      String topicName
  ) {
    return new KQLCsvSerializer();
  }
}
