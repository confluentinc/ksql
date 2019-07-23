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
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class DataGenProducer {

  static final ConnectSchema KEY_SCHEMA = (ConnectSchema) SchemaBuilder.struct()
      .field(SchemaUtil.ROWKEY_NAME, org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  // Max 100 ms between messsages.
  public static final long INTER_MESSAGE_MAX_INTERVAL = 500;

  private final KsqlSerdeFactory keySerializerFactory;
  private final KsqlSerdeFactory valueSerializerFactory;
  private final Supplier<SchemaRegistryClient> srClientSupplier;

  public DataGenProducer(
      final KsqlSerdeFactory keySerializerFactory,
      final KsqlSerdeFactory valueSerializerFactory,
      final Supplier<SchemaRegistryClient> srClientSupplier
  ) {
    this.keySerializerFactory = requireNonNull(keySerializerFactory, "keySerializerFactory");
    this.valueSerializerFactory = requireNonNull(valueSerializerFactory, "valueSerializerFactory");
    this.srClientSupplier = requireNonNull(srClientSupplier, "srClientSupplier");
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

    final AvroData avroData = new AvroData(1);
    final org.apache.kafka.connect.data.Schema ksqlSchema =
        DataGenSchemaUtil.getOptionalSchema(avroData.toConnectSchema(avroSchema));

    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        LogicalSchema.of(KEY_SCHEMA, ksqlSchema),
        SerdeOption.none()
    );

    final KsqlConfig ksqConfig = new KsqlConfig(props);

    final Serializer<String> keySerializer = getKeySerializer(ksqConfig);

    final Serializer<GenericRow> valueSerializer = getValueSerializer(physicalSchema, ksqConfig);

    final KafkaProducer<String, GenericRow> producer = new KafkaProducer<>(
        props,
        keySerializer,
        valueSerializer
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

  @SuppressWarnings("unchecked")
  private Serializer<String> getKeySerializer(final KsqlConfig ksqConfig) {
    final PersistenceSchema schema = keySerializerFactory.getFormat().supportsUnwrapping()
        ? PersistenceSchema.of((ConnectSchema) KEY_SCHEMA.fields().get(0).schema())
        : PersistenceSchema.of(KEY_SCHEMA);

    return (Serializer<String>) (Serializer)keySerializerFactory
        .createSerde(schema, ksqConfig, srClientSupplier)
        .serializer();
  }

  private Serializer<GenericRow> getValueSerializer(
      final PhysicalSchema physicalSchema,
      final KsqlConfig ksqConfig
  ) {
    return GenericRowSerDe.from(
          valueSerializerFactory,
          physicalSchema,
          ksqConfig,
          srClientSupplier,
          "",
          NoopProcessingLogContext.INSTANCE
      ).serializer();
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
}
