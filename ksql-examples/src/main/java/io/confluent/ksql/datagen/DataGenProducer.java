/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.datagen;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.physical.GenericRow;
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

  // Max 100 ms between messsages.
  public static final long INTER_MESSAGE_MAX_INTERVAL = 100;

  public void populateTopic(
      Properties props,
      Generator generator,
      String kafkaTopicName,
      String key,
      int messageCount,
      long maxInterval
  ) {
    if (maxInterval < 0) {
      maxInterval = INTER_MESSAGE_MAX_INTERVAL;
    }
    Schema avroSchema = generator.schema();
    org.apache.kafka.connect.data.Schema kafkaSchema = new AvroData(1).toConnectSchema(avroSchema);

    Serializer<GenericRow> serializer = getSerializer(avroSchema, kafkaSchema, kafkaTopicName);

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

      for (Schema.Field field : avroSchema.getFields()) {
        genericRowValues.add(randomAvroMessage.get(field.name()));
      }

      GenericRow genericRow = new GenericRow(genericRowValues);

      String keyString = randomAvroMessage.get(key).toString();

      ProducerRecord<String, GenericRow> producerRecord =
          new ProducerRecord<>(kafkaTopicName, keyString, genericRow);
      producer.send(producerRecord);
      System.err.println(keyString + " --> (" + genericRow + ")");
      try {
        Thread.sleep((long)(maxInterval * Math.random()));
      } catch (InterruptedException e) {
        // Ignore the exception.
      }
    }
    producer.flush();
    producer.close();
  }

  protected abstract Serializer<GenericRow> getSerializer(
      Schema avroSchema,
      org.apache.kafka.connect.data.Schema kafkaSchema,
      String topicName
  );

}