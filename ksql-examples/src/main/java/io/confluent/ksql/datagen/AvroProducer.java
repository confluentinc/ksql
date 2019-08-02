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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;

public class AvroProducer extends DataGenProducer {

  private final KsqlConfig ksqlConfig;
  private final SchemaRegistryClient schemaRegistryClient;

  public AvroProducer(final KsqlConfig ksqlConfig) {
    if (ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY) == null) {
      throw new KsqlException("Schema registry url is not set.");
    }
    this.ksqlConfig = ksqlConfig;
    this.schemaRegistryClient = new CachedSchemaRegistryClient(
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY),
        100,
        ksqlConfig.originalsWithPrefix(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX)
    );
  }

  @Override
  protected Serializer<GenericRow> getSerializer(
      final Schema avroSchema,
      final org.apache.kafka.connect.data.Schema kafkaSchema,
      final String topicName
  ) {
    final PersistenceSchema persistenceSchema =
        PersistenceSchema.from((ConnectSchema) kafkaSchema, false);

    return GenericRowSerDe.from(
        FormatInfo.of(Format.AVRO, Optional.empty()),
        persistenceSchema,
        ksqlConfig,
        () -> schemaRegistryClient,
        "",
        NoopProcessingLogContext.INSTANCE
    ).serializer();
  }
}
