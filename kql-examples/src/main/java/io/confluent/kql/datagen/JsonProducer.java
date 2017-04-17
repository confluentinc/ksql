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

  protected org.apache.kafka.connect.data.Schema getKQLSchema(String topicName) {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    switch (topicName.toUpperCase()) {
      case "ORDERS":
        return schemaBuilder.field("ordertime", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .field("orderid", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("itemid", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .field("orderunits", org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA);

      case "USERS":
        return schemaBuilder.field("registertime", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .field("userid", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("regionid", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .field("gender", org.apache.kafka.connect.data.Schema.STRING_SCHEMA);
      case "PAGEVIEW":
        return schemaBuilder.field("viewtime", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .field("userid", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("pageid", org.apache.kafka.connect.data.Schema.STRING_SCHEMA);
      default:
        throw new KQLException("Undefined topic for examples: " + topicName);
    }
  }
}
