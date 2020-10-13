/*
 * Copyright 2020 Confluent Inc.
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


package io.confluent.ksql.test.serde.none;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.serde.voids.KsqlVoidSerde;
import io.confluent.ksql.test.serde.SerdeSupplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class NoneSerdeSupplier implements SerdeSupplier<Void> {

  @Override
  public Serializer<Void> getSerializer(
      final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey
  ) {
    return new KsqlVoidSerde<Void>().serializer();
  }

  @Override
  public Deserializer<Void> getDeserializer(
      final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey
  ) {
    return new KsqlVoidSerde<Void>().deserializer();
  }
} 