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

package io.confluent.ksql.test.commons;

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class Topic {
  final String name;
  private final Optional<Schema> schema;
  private final SerdeSupplier serdeSupplier;
  final int numPartitions;
  final int replicas;

  public Topic(
      final String name,
      final Optional<Schema> schema,
      final SerdeSupplier serdeSupplier,
      final int numPartitions,
      final int replicas
  ) {
    this.name = requireNonNull(name, "name");
    this.schema = requireNonNull(schema, "schema");
    this.serdeSupplier = requireNonNull(serdeSupplier, "serdeSupplier");
    this.numPartitions = numPartitions;
    this.replicas = replicas;
  }

  public String getName() {
    return name;
  }

  Optional<Schema> getSchema() {
    return schema;
  }

  public SerdeSupplier getSerdeSupplier() {
    return serdeSupplier;
  }

  Serializer getSerializer(final SchemaRegistryClient schemaRegistryClient) {
    return serdeSupplier.getSerializer(schemaRegistryClient);
  }

  Deserializer getDeserializer(final SchemaRegistryClient schemaRegistryClient) {
    return serdeSupplier.getDeserializer(schemaRegistryClient);
  }
}