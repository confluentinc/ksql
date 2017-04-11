/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.kql.datagen;

import io.confluent.avro.random.generator.Generator;

import io.confluent.kql.physical.GenericRow;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

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

    Serializer<GenericRow> serializer = getSerializer(schema);

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

  protected abstract Serializer<GenericRow> getSerializer(Schema schema);
}