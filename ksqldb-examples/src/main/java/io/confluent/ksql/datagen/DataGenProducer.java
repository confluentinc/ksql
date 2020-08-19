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

import com.google.common.util.concurrent.RateLimiter;
import io.confluent.avro.random.generator.Generator;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.util.Pair;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Struct;

@SuppressWarnings("UnstableApiUsage")
public class DataGenProducer {

  private final SerializerFactory<Struct> keySerializerFactory;
  private final SerializerFactory<GenericRow> valueSerializerFactory;

  public DataGenProducer(
      final SerializerFactory<Struct> keySerializerFactory,
      final SerializerFactory<GenericRow> valueSerdeFactory
  ) {
    this.keySerializerFactory = requireNonNull(keySerializerFactory, "keySerializerFactory");
    this.valueSerializerFactory = requireNonNull(valueSerdeFactory, "valueSerdeFactory");
  }

  // Protected for test purpose.
  protected static void validateTimestampColumnType(
      final Optional<String> timestampColumnName,
      final Schema avroSchema
  ) {
    if (timestampColumnName.isPresent()) {
      if (avroSchema.getField(timestampColumnName.get()) == null) {
        throw new IllegalArgumentException("The indicated timestamp field does not exist: "
            + timestampColumnName.get());
      }
      if (avroSchema.getField(timestampColumnName.get()).schema().getType() != Type.LONG) {
        throw new IllegalArgumentException("The timestamp column type should be bigint/long. "
            + timestampColumnName.get() + " type is "
            + avroSchema.getField(timestampColumnName.get()).schema().getType());
      }
    }
  }

  @SuppressWarnings("InfiniteLoopStatement")
  public void populateTopic(
      final Properties props,
      final Generator generator,
      final String kafkaTopicName,
      final String key,
      final Optional<String> timestampColumnName,
      final int messageCount,
      final boolean printRows,
      final Optional<RateLimiter> rateLimiter
  ) {
    final Schema avroSchema = generator.schema();
    if (avroSchema.getField(key) == null) {
      throw new IllegalArgumentException("Key field does not exist: " + key);
    }

    validateTimestampColumnType(timestampColumnName, avroSchema);


    final RowGenerator rowGenerator = new RowGenerator(generator, key, timestampColumnName);

    final Serializer<Struct> keySerializer =
        getKeySerializer(rowGenerator.keySchema());

    final Serializer<GenericRow> valueSerializer =
        getValueSerializer(rowGenerator.valueSchema());

    final KafkaProducer<Struct, GenericRow> producer = new KafkaProducer<>(
        props,
        keySerializer,
        valueSerializer
    );

    if (messageCount != -1) {
      for (int i = 0; i < messageCount; i++) {
        produceOne(
            rowGenerator,
            producer,
            kafkaTopicName,
            printRows,
            rateLimiter
        );
      }
    } else {
      while (true) {
        produceOne(
            rowGenerator,
            producer,
            kafkaTopicName,
            printRows,
            rateLimiter
        );
      }
    }

    producer.flush();
    producer.close();
  }

  private static void produceOne(
      final RowGenerator rowGenerator,
      final KafkaProducer<Struct, GenericRow> producer,
      final String kafkaTopicName,
      final boolean printRows,
      final Optional<RateLimiter> rateLimiter
  ) {
    rateLimiter.ifPresent(RateLimiter::acquire);

    final Pair<Struct, GenericRow> genericRowPair = rowGenerator.generateRow();
    final Long timestamp = rowGenerator.getTimestampFieldIndex().isPresent()
        ? (Long) genericRowPair.getRight().get(rowGenerator.getTimestampFieldIndex().get())
        : null;

    final ProducerRecord<Struct, GenericRow> producerRecord = new ProducerRecord<>(
        kafkaTopicName,
        (Integer) null,
        timestamp,
        genericRowPair.getLeft(),
        genericRowPair.getRight()
    );

    producer.send(producerRecord,
        new LoggingCallback(kafkaTopicName,
            genericRowPair.getLeft(),
            genericRowPair.getRight(),
            printRows));
  }

  private Serializer<Struct> getKeySerializer(
      final ConnectSchema keySchema
  ) {
    final PersistenceSchema schema = PersistenceSchema.from(
        keySchema,
        keySerializerFactory.format().supportedFeatures().contains(SerdeFeature.UNWRAP_SINGLES)
    );

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
    private final boolean printOnSuccess;

    LoggingCallback(
        final String topic,
        final Struct key,
        final GenericRow value,
        final boolean printOnSuccess) {
      this.topic = topic;
      this.key = formatKey(key);
      this.value = Objects.toString(value);
      this.printOnSuccess = printOnSuccess;
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
        if (printOnSuccess) {
          System.out.println(key + " --> (" + value + ") ts:" + metadata.timestamp());
        }
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
