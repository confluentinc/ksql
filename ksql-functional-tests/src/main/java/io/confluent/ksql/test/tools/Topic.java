/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.tools;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde;

@SuppressWarnings("rawtypes")
public class Topic {

  private final String name;
  private final Optional<Schema> schema;
  private final SerdeFactory keySerdeFactory;
  private final SerdeSupplier valueSerdeSupplier;
  private final int numPartitions;
  private final short replicas;
  private final Optional<Long> windowSize;

  public Topic(
      final String name,
      final Optional<Schema> schema,
      final SerdeFactory keySerdeFactory,
      final SerdeSupplier valueSerdeSupplier,
      final int numPartitions,
      final int replicas,
      final Optional<Long> windowSize
  ) {
    this.name = requireNonNull(name, "name");
    this.schema = requireNonNull(schema, "schema");
    this.keySerdeFactory = requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.valueSerdeSupplier = requireNonNull(valueSerdeSupplier, "valueSerdeSupplier");
    this.numPartitions = numPartitions;
    this.replicas = (short) replicas;
    this.windowSize = requireNonNull(windowSize, "windowSize");
  }

  public String getName() {
    return name;
  }

  Optional<Schema> getSchema() {
    return schema;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public short getReplicas() {
    return replicas;
  }

  public SerdeSupplier getValueSerdeSupplier() {
    return valueSerdeSupplier;
  }

  public SerdeFactory getKeySerdeFactory() {
    return keySerdeFactory;
  }

  Serializer getValueSerializer(final SchemaRegistryClient schemaRegistryClient) {
    final Serializer<?> serializer = valueSerdeSupplier.getSerializer(schemaRegistryClient);
    serializer.configure(ImmutableMap.of(
        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
    ), false);
    return serializer;
  }

  Deserializer getValueDeserializer(final SchemaRegistryClient schemaRegistryClient) {
    final Deserializer<?> deserializer = valueSerdeSupplier.getDeserializer(schemaRegistryClient);
    deserializer.configure(ImmutableMap.of(), false);
    return deserializer;
  }

  Serializer getKeySerializer() {
    return keySerdeFactory.create().serializer();
  }

  Deserializer<?> getKeyDeserializer() {
    final Serde keySerde = keySerdeFactory.create();
    if (!(keySerde instanceof TimeWindowedSerde)) {
      return keySerde.deserializer();
    }

    if (windowSize.isPresent()) {
      return new TimeWindowedDeserializer<>(new StringDeserializer(), windowSize.get());
    }

    final TimeWindowedSerde windowedSerde = (TimeWindowedSerde) keySerde;
    final TimeWindowedDeserializer timeWindowedDeserializer =
        (TimeWindowedDeserializer) windowedSerde.deserializer();
    if (timeWindowedDeserializer.getWindowSize() == Long.MAX_VALUE) {
      throw new KsqlException("Window size is not present for time windowed deserializer.");
    }

    return new TimeWindowedDeserializer<>(
        new StringDeserializer(),
        timeWindowedDeserializer.getWindowSize());
  }
}