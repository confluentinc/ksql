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
import java.util.Optional;

public class Topic {

  private final String name;
  private final int numPartitions;
  private final short replicas;
  private final Optional<ParsedSchema> schema;

  public Topic(
      final String name,
      final int numPartitions,
      final int replicas,
      final Optional<ParsedSchema> schema
  ) {
    this.name = requireNonNull(name, "name");
    this.schema = requireNonNull(schema, "schema");
    this.numPartitions = numPartitions;
    this.replicas = (short) replicas;
  }

  public String getName() {
    return name;
  }

  public Optional<ParsedSchema> getSchema() {
    return schema;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public short getReplicas() {
    return replicas;
  }
}
