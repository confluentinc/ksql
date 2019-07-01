/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.datagen;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.SchemaBuilder;

public abstract class DataGenProducer {

  static final org.apache.kafka.connect.data.Schema KEY_SCHEMA = SchemaBuilder.struct()
      .field(SchemaUtil.ROWKEY_NAME, org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  // Max 100 ms between messsages.
  public static final long INTER_MESSAGE_MAX_INTERVAL = 500;

  public void populateTopic(
      final Properties props,
      final Generator generator,
      final String kafkaTopicName,
      final String key,
      final int messageCount,
      final long maxInterval
  ) {
    final Schema avroSchema = generator.schema();
    if (avroSchema.getField(key) == null) {
      throw new IllegalArgumentException("Key field does not exist:" + key);
    }

    final AvroData avroData = new AvroData(1);
    final org.apache.kafka.connect.data.Schema ksqlSchema =
        DataGenSchemaUtil.getOptionalSchema(avroData.toConnectSchema(avroSchema));

    final Serializer<GenericRow> serializer = getSerializer(avroSchema, ksqlSchema, kafkaTopicName);

    final KafkaProducer<String, GenericRow> producer = new KafkaProducer<>(
        props,
        new StringSerializer(),
        serializer
    );

    final SessionManager sessionManager = new SessionManager();
    final RowGenerator rowGenerator =
        new RowGenerator(generator, avroData, avroSchema, ksqlSchema, sessionManager, key);

    for (int i = 0; i < messageCount; i++) {

      final Pair<String, GenericRow> genericRowPair = rowGenerator.generateRow();

      final ProducerRecord<String, GenericRow> producerRecord = new ProducerRecord<>(
          kafkaTopicName,
          genericRowPair.getLeft(),
          genericRowPair.getRight()
      );

      producer.send(producerRecord,
          new ErrorLoggingCallback(kafkaTopicName,
              genericRowPair.getLeft(),
              genericRowPair.getRight()));

      try {
        final long interval = maxInterval < 0 ? INTER_MESSAGE_MAX_INTERVAL : maxInterval;

        Thread.sleep((long) (interval * Math.random()));
      } catch (final InterruptedException e) {
        // Ignore the exception.
      }
    }
    producer.flush();
    producer.close();
  }

  private static class ErrorLoggingCallback implements Callback {

    private final String topic;
    private final String key;
    private final GenericRow value;

    ErrorLoggingCallback(final String topic, final String key, final GenericRow value) {
      this.topic = topic;
      this.key = key;
      this.value = value;
    }

    @Override
    public void onCompletion(final RecordMetadata metadata, final Exception e) {
      final String keyString = Objects.toString(key);
      final String valueString = Objects.toString(value);

      if (e != null) {
        System.err.println("Error when sending message to topic: '" + topic + "', with key: '"
            + keyString + "', and value: '" + valueString + "'");
        e.printStackTrace(System.err);
      } else {
        System.out.println(keyString + " --> (" + valueString + ") ts:" + metadata.timestamp());
      }
    }
  }

  protected abstract Serializer<GenericRow> getSerializer(
      Schema avroSchema,
      org.apache.kafka.connect.data.Schema kafkaSchema,
      String topicName
  );
}
