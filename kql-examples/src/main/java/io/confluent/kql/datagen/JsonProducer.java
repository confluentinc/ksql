/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.datagen;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.serde.json.KQLJsonPOJOSerializer;
import io.confluent.kql.util.KQLException;

import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.SchemaBuilder;

public class JsonProducer extends DataGenProducer {

  @Override
  protected Serializer<GenericRow> getSerializer(Schema schema, String topicName) {
    return new KQLJsonPOJOSerializer(getKQLSchema(topicName));
  }


}
