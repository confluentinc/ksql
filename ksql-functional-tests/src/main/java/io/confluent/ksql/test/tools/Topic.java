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
import io.confluent.ksql.test.serde.SerdeSupplier;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

@SuppressWarnings("rawtypes")
public class Topic {

  private final String name;
  private final Optional<Schema> schema;
  private final SerdeSupplier keySerdeFactory;
  private final SerdeSupplier valueSerdeSupplier;
  private final int numPartitions;
  private final short replicas;

  public Topic(
      final String name,
      final Optional<Schema> schema,
      final SerdeSupplier keySerdeFactory,
      final SerdeSupplier valueSerdeSupplier,
      final int numPartitions,
      final int replicas
  ) {
    this.name = requireNonNull(name, "name");
    this.schema = requireNonNull(schema, "schema");
    this.keySerdeFactory = requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.valueSerdeSupplier = requireNonNull(valueSerdeSupplier, "valueSerdeSupplier");
    this.numPartitions = numPartitions;
    this.replicas = (short) replicas;
  }

  public String getName() {
    return name;
  }

  public Optional<Schema> getSchema() {
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

  public Serializer getValueSerializer(final SchemaRegistryClient schemaRegistryClient) {
    final Serializer<?> serializer = valueSerdeSupplier.getSerializer(schemaRegistryClient);
    serializer.configure(ImmutableMap.of(
        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
    ), false);
    return serializer;
  }

  public Deserializer getValueDeserializer(final SchemaRegistryClient schemaRegistryClient) {
    final Deserializer<?> deserializer = valueSerdeSupplier.getDeserializer(schemaRegistryClient);
    deserializer.configure(ImmutableMap.of(
        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "foo"
    ), false);
    return deserializer;
  }

  public Serializer getKeySerializer(final SchemaRegistryClient schemaRegistryClient) {
    final Serializer<?> serializer = keySerdeFactory.getSerializer(schemaRegistryClient);
    serializer.configure(ImmutableMap.of(
        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "something"
    ), true);
    return serializer;
  }

  public Deserializer<?> getKeyDeserializer(final SchemaRegistryClient schemaRegistryClient) {
    final Deserializer<?> deserializer = keySerdeFactory.getDeserializer(schemaRegistryClient);
    deserializer.configure(ImmutableMap.of(), true);
    return deserializer;
  }
}
