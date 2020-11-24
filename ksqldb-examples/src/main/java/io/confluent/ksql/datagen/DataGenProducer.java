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
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.Pair;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

@SuppressWarnings("UnstableApiUsage")
public class DataGenProducer {

  private final SerializerFactory<GenericKey> keySerializerFactory;
  private final SerializerFactory<GenericRow> valueSerializerFactory;

  public DataGenProducer(
      final SerializerFactory<GenericKey> keySerializerFactory,
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

    final Serializer<GenericKey> keySerializer =
        getKeySerializer(rowGenerator.schema());

    final Serializer<GenericRow> valueSerializer =
        getValueSerializer(rowGenerator.schema());

    final KafkaProducer<GenericKey, GenericRow> producer = new KafkaProducer<>(
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
      final KafkaProducer<GenericKey, GenericRow> producer,
      final String kafkaTopicName,
      final boolean printRows,
      final Optional<RateLimiter> rateLimiter
  ) {
    rateLimiter.ifPresent(RateLimiter::acquire);

    final Pair<GenericKey, GenericRow> genericRowPair = rowGenerator.generateRow();
    final Long timestamp = rowGenerator.getTimestampFieldIndex().isPresent()
        ? (Long) genericRowPair.getRight().get(rowGenerator.getTimestampFieldIndex().get())
        : null;

    final ProducerRecord<GenericKey, GenericRow> producerRecord = new ProducerRecord<>(
        kafkaTopicName,
        null,
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

  private Serializer<GenericKey> getKeySerializer(final LogicalSchema schema) {
    final Set<SerdeFeature> supported = keySerializerFactory.format().supportedFeatures();
    final SerdeFeatures features = supported.contains(SerdeFeature.UNWRAP_SINGLES)
        ? SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)
        : SerdeFeatures.of();

    final PersistenceSchema persistenceSchema = PersistenceSchema.from(schema.key(), features);
    return keySerializerFactory.create(persistenceSchema);
  }

  private Serializer<GenericRow> getValueSerializer(final LogicalSchema schema) {
    final PersistenceSchema persistenceSchema = PersistenceSchema
        .from(schema.value(), SerdeFeatures.of());
    return valueSerializerFactory.create(persistenceSchema);
  }

  private static class LoggingCallback implements Callback {

    private final String topic;
    private final String key;
    private final String value;
    private final boolean printOnSuccess;

    LoggingCallback(
        final String topic,
        final GenericKey key,
        final GenericRow value,
        final boolean printOnSuccess) {
      this.topic = topic;
      this.key = Objects.toString(key);
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
  }
}
