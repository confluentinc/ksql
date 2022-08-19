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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.serde.SerdeFeatures;
import java.util.Objects;
import java.util.Optional;

public class Topic {

  public static final int DEFAULT_PARTITIONS = 4;
  public static final short DEFAULT_RF = 1;

  private final String name;
  private final int numPartitions;
  private final short replicas;
  final Optional<Integer> keySchemaId;
  final Optional<Integer> valueSchemaId;
  private final Optional<ParsedSchema> keySchema;
  private final Optional<ParsedSchema> valueSchema;
  private final SerdeFeatures keyFeatures;
  private final SerdeFeatures valueFeatures;

  public Topic(
      final String name,
      final Optional<ParsedSchema> keySchema,
      final Optional<ParsedSchema> valueSchema
  ) {
    this(name, DEFAULT_PARTITIONS, DEFAULT_RF, Optional.empty(), Optional.empty(),
        keySchema, valueSchema, SerdeFeatures.of(), SerdeFeatures.of());
  }

  public Topic(
      final String name,
      final int numPartitions,
      final int replicas,
      final Optional<Integer> keySchemaId,
      final Optional<Integer> valueSchemaId,
      final Optional<ParsedSchema> keySchema,
      final Optional<ParsedSchema> valueSchema,
      final SerdeFeatures keyFeatures,
      final SerdeFeatures valueFeatures
  ) {
    this.name = requireNonNull(name, "name");
    this.keySchemaId = requireNonNull(keySchemaId, "keySchemaId");
    this.valueSchemaId = requireNonNull(valueSchemaId, "valueSchemaId");
    this.keySchema = requireNonNull(keySchema, "keySchema");
    this.valueSchema = requireNonNull(valueSchema, "valueSchema");
    this.numPartitions = numPartitions;
    this.replicas = (short) replicas;
    this.keyFeatures = keyFeatures;
    this.valueFeatures = valueFeatures;
  }

  public String getName() {
    return name;
  }

  public Optional<Integer> getKeySchemaId() {
    return keySchemaId;
  }

  public Optional<Integer> getValueSchemaId() {
    return valueSchemaId;
  }

  public Optional<ParsedSchema> getKeySchema() {
    return keySchema;
  }

  public Optional<ParsedSchema> getValueSchema() {
    return valueSchema;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public short getReplicas() {
    return replicas;
  }

  public SerdeFeatures getKeyFeatures() {
    return keyFeatures;
  }

  public SerdeFeatures getValueFeatures() {
    return valueFeatures;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Topic topic = (Topic) o;
    return numPartitions == topic.numPartitions
        && replicas == topic.replicas
        && Objects.equals(name, topic.name)
        && Objects.equals(keySchemaId, topic.keySchemaId)
        && Objects.equals(valueSchemaId, topic.valueSchemaId)
        && Objects.equals(keySchema, topic.keySchema)
        && Objects.equals(valueSchema, topic.valueSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, numPartitions, replicas, keySchemaId,
        valueSchemaId, keySchema, valueSchema);
  }
}
