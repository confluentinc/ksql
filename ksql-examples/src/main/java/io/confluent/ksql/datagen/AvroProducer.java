/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/


package io.confluent.ksql.datagen;

import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serializer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.avro.KsqlGenericRowAvroSerializer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

public class AvroProducer extends DataGenProducer {

  private final KsqlConfig ksqlConfig;
  private final SchemaRegistryClient schemaRegistryClient;

  public AvroProducer(KsqlConfig ksqlConfig) {
    if (ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY) == null) {
      throw new KsqlException("Schema registry url is not set.");
    }
    this.ksqlConfig = ksqlConfig;
    this.schemaRegistryClient = new CachedSchemaRegistryClient(
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY),
        100
    );
  }

  @Override
  protected Serializer<GenericRow> getSerializer(
      Schema avroSchema,
      org.apache.kafka.connect.data.Schema kafkaSchema,
      String topicName
  ) {
    return new KsqlGenericRowAvroSerializer(kafkaSchema, schemaRegistryClient, ksqlConfig);
  }
}
