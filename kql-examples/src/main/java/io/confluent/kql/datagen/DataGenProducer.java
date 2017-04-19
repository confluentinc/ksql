/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.datagen;

import io.confluent.avro.random.generator.Generator;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.util.KQLException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class DataGenProducer {


  public void populateTopic(
      Properties props,
      Generator generator,
      String kafkaTopicName,
      String key,
      int messageCount
  ) {
    Schema schema = generator.schema();

    Serializer<GenericRow> serializer = getSerializer(schema, kafkaTopicName);

    final KafkaProducer<String, GenericRow> producer = new KafkaProducer<>(props, new StringSerializer(), serializer);

    for (int i = 0; i < messageCount; i++) {
      Object generatedObject = generator.generate();

      if (!(generatedObject instanceof GenericRecord)) {
        throw new RuntimeException(String.format(
            "Expected Avro Random Generator to return instance of GenericRecord, found %s instead",
            generatedObject.getClass().getName()
        ));
      }
      GenericRecord randomAvroMessage = (GenericRecord) generatedObject;

      List<Object> genericRowValues = new ArrayList<>();

      for (Schema.Field field : schema.getFields()) {
        genericRowValues.add(randomAvroMessage.get(field.name()));
      }

      GenericRow genericRow = new GenericRow(genericRowValues);

      String keyString = randomAvroMessage.get(key).toString();

      ProducerRecord<String, GenericRow> producerRecord =
          new ProducerRecord<>(kafkaTopicName, keyString, genericRow);
      producer.send(producerRecord);
      System.err.println(keyString + " --> (" + genericRow + ")");
    }
    producer.flush();
    producer.close();

    System.err.println("Done!");
    System.err.println("Kafka topic name: " + kafkaTopicName);
  }

  protected abstract Serializer<GenericRow> getSerializer(Schema schema, String topicName);

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