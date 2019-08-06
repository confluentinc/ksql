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

import static java.util.Objects.requireNonNull;

import io.confluent.avro.random.generator.Generator;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.Pair;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Struct;

public class DataGenProducer {

  // Max 100 ms between messsages.
  public static final long INTER_MESSAGE_MAX_INTERVAL = 500;

  private final SerializerFactory<Struct> keySerializerFactory;
  private final SerializerFactory<GenericRow> valueSerializerFactory;

  public DataGenProducer(
      final SerializerFactory<Struct> keySerializerFactory,
      final SerializerFactory<GenericRow> valueSerdeFactory
  ) {
    this.keySerializerFactory = requireNonNull(keySerializerFactory, "keySerializerFactory");
    this.valueSerializerFactory = requireNonNull(valueSerdeFactory, "valueSerdeFactory");
  }

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

    final RowGenerator rowGenerator = new RowGenerator(generator, key);

    final Serializer<Struct> keySerializer = getKeySerializer();

    final Serializer<GenericRow> valueSerializer =
        getValueSerializer(rowGenerator.schema().valueSchema());

    final KafkaProducer<Struct, GenericRow> producer = new KafkaProducer<>(
        props,
        keySerializer,
        valueSerializer
    );

    for (int i = 0; i < messageCount; i++) {

      final Pair<Struct, GenericRow> genericRowPair = rowGenerator.generateRow();

      final ProducerRecord<Struct, GenericRow> producerRecord = new ProducerRecord<>(
          kafkaTopicName,
          genericRowPair.getLeft(),
          genericRowPair.getRight()
      );

      producer.send(producerRecord,
          new LoggingCallback(kafkaTopicName,
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

  private Serializer<Struct> getKeySerializer() {
    final PersistenceSchema schema = PersistenceSchema
        .from(RowGenerator.KEY_SCHEMA, keySerializerFactory.format().supportsUnwrapping());

    return keySerializerFactory.create(schema);
  }

  private Serializer<GenericRow> getValueSerializer(final ConnectSchema valueSchema) {
    final PersistenceSchema schema = PersistenceSchema
        .from(valueSchema, false);

    return valueSerializerFactory.create(schema);
  }

  private static class LoggingCallback implements Callback {

    private final String topic;
    private final String key;
    private final String value;

    LoggingCallback(final String topic, final Struct key, final GenericRow value) {
      this.topic = topic;
      this.key = formatKey(key);
      this.value = Objects.toString(value);
    }

    @Override
    public void onCompletion(final RecordMetadata metadata, final Exception e) {
      if (e != null) {
        System.err.println(
            "Error when sending message to topic: '" + topic + "', "
                + "with key: '" + key + "', "
                + "and value: '" + value + "'"
        );
        e.printStackTrace(System.err);
      } else {
        System.out.println(key + " --> (" + value + ") ts:" + metadata.timestamp());
      }
    }

    private static String formatKey(final Struct key) {
      if (key == null) {
        return "null";
      }

      return key.schema().fields().stream()
          .map(f -> Objects.toString(key.get(f)))
          .collect(Collectors.joining(" | "));
    }
  }
}
