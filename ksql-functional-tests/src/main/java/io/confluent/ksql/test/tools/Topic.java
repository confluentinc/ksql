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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde;

@SuppressWarnings("rawtypes")
public class Topic {
  final String name;
  private final Optional<Schema> schema;
  private final SerdeFactory keySerdeFactory;
  private final SerdeSupplier valueSserdeSupplier;
  final int numPartitions;
  final int replicas;
  final Optional<Long> windowSize;

  public Topic(
      final String name,
      final Optional<Schema> schema,
      final SerdeSupplier valueSserdeSupplier,
      final int numPartitions,
      final int replicas
  ) {
    this(
        name,
        schema,
        Serdes::String,
        valueSserdeSupplier,
        numPartitions,
        replicas,
        Optional.empty()
    );
  }

  public Topic(
      final String name,
      final Optional<Schema> schema,
      final SerdeFactory keySerdeFactory,
      final SerdeSupplier valueSserdeSupplier,
      final int numPartitions,
      final int replicas,
      final Optional<Long> windowSize
  ) {
    this.name = requireNonNull(name, "name");
    this.schema = requireNonNull(schema, "schema");
    this.keySerdeFactory = requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.valueSserdeSupplier = requireNonNull(valueSserdeSupplier, "valueSserdeSupplier");
    this.numPartitions = numPartitions;
    this.replicas = replicas;
    this.windowSize = requireNonNull(windowSize, "windowSize");
  }

  public String getName() {
    return name;
  }

  Optional<Schema> getSchema() {
    return schema;
  }

  public SerdeSupplier getValueSerdeSupplier() {
    return valueSserdeSupplier;
  }

  public SerdeFactory getKeySerdeFactory() {
    return keySerdeFactory;
  }

  Serializer getValueSerializer(final SchemaRegistryClient schemaRegistryClient) {
    return valueSserdeSupplier.getSerializer(schemaRegistryClient);
  }

  Deserializer getValueDeserializer(final SchemaRegistryClient schemaRegistryClient) {
    return valueSserdeSupplier.getDeserializer(schemaRegistryClient);
  }

  Serializer getKeySerializer() {
    return keySerdeFactory.create().serializer();
  }

  Deserializer getKeyDeserializer() {
    final Serde keySerde = keySerdeFactory.create();
    if (keySerde instanceof TimeWindowedSerde) {
      final TimeWindowedSerde windowedSerde = (TimeWindowedSerde) keySerde;
      final TimeWindowedDeserializer timeWindowedDeserializer =
          (TimeWindowedDeserializer) windowedSerde.deserializer();
      if (!windowSize.isPresent()) {
        if (timeWindowedDeserializer.getWindowSize() != Long.MAX_VALUE) {
          return new TimeWindowedDeserializer<>(
              new StringDeserializer(),
              timeWindowedDeserializer.getWindowSize());
        }
        throw new KsqlException("Window size is not present for time windowed deserializer.");
      }
      return new TimeWindowedDeserializer<>(new StringDeserializer(), windowSize.get());
    }
    return keySerde.deserializer();
  }
}